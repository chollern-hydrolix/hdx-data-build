include .env
export

PROFILE_NAME = AdministratorAccess-570204184505
AWS_REGION ?= us-east-2
ECR_REPO   ?= hdx-business-systems/data-build
IMAGE_TAG  ?= latest

DBT = cd dbt_analytics && dbt

.PHONY: dbt-deps dbt-debug dbt-run dbt-test dbt-clean dbt-build dbt-run-model dbt-build-model dbt-snapshot dbt-snapshot-select \
        docker-build docker-push ecr-login register-tasks deploy \
        tmp-create-execution-role tmp-create-task-role \
        tmp-attach-execution-policy tmp-attach-task-policy \
        tmp-update-execution-trust tmp-update-task-trust \
        tmp-create-log-groups tmp-run-crm \
        tmp-create-scheduler-role tmp-attach-scheduler-policy

# --- dbt (local) ---

dbt-deps:
	$(DBT) deps

dbt-debug:
	$(DBT) debug

dbt-run:
	$(DBT) run

dbt-build:
	$(DBT) build

dbt-test:
	$(DBT) test

dbt-clean:
	$(DBT) clean

dbt-run-model:
	$(DBT) run -s $(model)

dbt-build-model:
	$(DBT) build -s $(model)

dbt-snapshot:
	$(DBT) snapshot

dbt-snapshot-select:
	$(DBT) snapshot -s $(snapshot)

dbt-docs-generate:
	$(DBT) docs generate --target $(env)

dbt-docs-serve:
	$(DBT) docs serve --host 127.0.0.1

dbt-docs-export:
	python scripts/generate_docs.py --dbt-target $(env)

# --- Docker / ECR ---

ecr-login:
	aws ecr get-login-password --region $(AWS_REGION) --profile $(PROFILE_NAME) | \
	  docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

docker-build:
	docker build --platform linux/amd64 -t $(ECR_REPO):$(IMAGE_TAG) .

docker-push: ecr-login
	docker tag $(ECR_REPO):$(IMAGE_TAG) \
	  $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(ECR_REPO):$(IMAGE_TAG)
	docker push \
	  $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(ECR_REPO):$(IMAGE_TAG)

# Register all ECS task definitions
register-tasks:
	for td in ecs/task_definitions/*.json; do \
	  echo "Registering $$td..."; \
	  aws ecs register-task-definition \
	    --cli-input-json file://$$td \
	 	--profile $(PROFILE_NAME) \
	    --region $(AWS_REGION) \
	    --no-cli-pager; \
	done

deploy: docker-build docker-push register-tasks dbt-docs-export

# --- One-time AWS setup (run once to bootstrap IAM roles and log groups) ---

tmp-create-execution-role:
	aws iam create-role \
	  --role-name ecsTaskExecutionRole \
	  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
	  --profile $(PROFILE_NAME) --no-cli-pager

tmp-create-task-role:
	aws iam create-role \
	  --role-name ecsTaskRole \
	  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
	  --profile $(PROFILE_NAME) --no-cli-pager

tmp-attach-execution-policy:
	aws iam attach-role-policy \
	  --role-name ecsTaskExecutionRole \
	  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy \
	  --profile $(PROFILE_NAME)
	aws iam attach-role-policy \
	  --role-name ecsTaskExecutionRole \
	  --policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite \
	  --profile $(PROFILE_NAME)

tmp-attach-task-policy:
	aws iam attach-role-policy \
	  --role-name ecsTaskRole \
	  --policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite \
	  --profile $(PROFILE_NAME)

tmp-update-execution-trust:
	aws iam update-assume-role-policy \
	  --role-name ecsTaskExecutionRole \
	  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
	  --profile $(PROFILE_NAME)

tmp-update-task-trust:
	aws iam update-assume-role-policy \
	  --role-name ecsTaskRole \
	  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
	  --profile $(PROFILE_NAME)

tmp-create-log-groups:
	aws logs create-log-group --log-group-name /ecs/dbt-run-crm --region $(AWS_REGION) --profile $(PROFILE_NAME)
	aws logs create-log-group --log-group-name /ecs/dbt-build-finance --region $(AWS_REGION) --profile $(PROFILE_NAME)
	aws logs create-log-group --log-group-name /ecs/dbt-build-mart-monthly-customer-usage --region $(AWS_REGION) --profile $(PROFILE_NAME)
	aws logs create-log-group --log-group-name /ecs/dbt-build-medium-priority --region $(AWS_REGION) --profile $(PROFILE_NAME)
	aws logs create-log-group --log-group-name /ecs/dbt-snapshot --region $(AWS_REGION) --profile $(PROFILE_NAME)

tmp-create-scheduler-role:
	aws iam create-role \
	  --role-name Amazon_EventBridge_Scheduler_ECS_Role \
	  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
	  --profile $(PROFILE_NAME) --no-cli-pager

tmp-attach-scheduler-policy:
	aws iam put-role-policy \
	  --role-name Amazon_EventBridge_Scheduler_ECS_Role \
	  --policy-name ECSRunTaskPolicy \
	  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ecs:RunTask","Resource":"arn:aws:ecs:$(AWS_REGION):$(AWS_ACCOUNT_ID):task-definition/*"},{"Effect":"Allow","Action":"iam:PassRole","Resource":["arn:aws:iam::$(AWS_ACCOUNT_ID):role/ecsTaskExecutionRole","arn:aws:iam::$(AWS_ACCOUNT_ID):role/ecsTaskRole"]}]}' \
	  --profile $(PROFILE_NAME) --no-cli-pager

tmp-run-crm:
	aws ecs run-task \
	  --cluster $(ECS_CLUSTER) \
	  --task-definition dbt-run-crm \
	  --launch-type FARGATE \
	  --network-configuration "awsvpcConfiguration={subnets=[$(SUBNET_IDS)],securityGroups=[$(SECURITY_GROUP_IDS)],assignPublicIp=ENABLED}" \
	  --region $(AWS_REGION) \
	  --profile $(PROFILE_NAME) \
	  --no-cli-pager

tmp-build-finance:
	aws ecs run-task \
	  --cluster $(ECS_CLUSTER) \
	  --task-definition dbt-build-finance \
	  --launch-type FARGATE \
	  --network-configuration "awsvpcConfiguration={subnets=[$(SUBNET_IDS)],securityGroups=[$(SECURITY_GROUP_IDS)],assignPublicIp=ENABLED}" \
	  --region $(AWS_REGION) \
	  --profile $(PROFILE_NAME) \
	  --no-cli-pager
