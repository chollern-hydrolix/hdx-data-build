import boto3
import yaml
import json
import os

PATH = 'tasks.yml'

flexible_time_window = {'Mode': 'OFF'}
max_retries = 0


def load_schedules():
    file_path = os.path.join(os.path.dirname(__file__), PATH)
    with open(file_path, 'r') as fin:
        data = yaml.safe_load(fin)
        return data.get('schedules', [])


def build_target(target_def: dict) -> dict:
    ecs_params = target_def.get('ecs_parameters', {})
    return {
        'Arn': target_def.get('arn'),
        'RoleArn': target_def.get('role_arn'),
        'EcsParameters': {
            'TaskDefinitionArn': ecs_params.get('task_definition_arn'),
            'LaunchType': 'FARGATE',
            'NetworkConfiguration': {
                'awsvpcConfiguration': {
                    'Subnets': ecs_params.get('subnet_ids', []),
                    'SecurityGroups': ecs_params.get('security_group_ids', []),
                    'AssignPublicIp': 'ENABLED',
                }
            },
            'TaskCount': 1,
        },
        'RetryPolicy': {'MaximumRetryAttempts': max_retries},
    }


def deploy_schedule(scheduler_client: boto3.client, schedule_definition: dict):
    name = schedule_definition.get('name')
    target = build_target(schedule_definition.get('target', {}))

    kwargs = {
        'Name': name,
        'Description': schedule_definition.get('description', ''),
        'ScheduleExpression': schedule_definition.get('schedule_expression'),
        'ScheduleExpressionTimezone': 'UTC',
        'State': schedule_definition.get('state', 'ENABLED'),
        'FlexibleTimeWindow': flexible_time_window,
        'GroupName': schedule_definition.get('group_name'),
        'Target': target,
    }

    try:
        print(f'Deploying schedule: {name}')
        scheduler_client.create_schedule(**kwargs)
    except scheduler_client.exceptions.ConflictException:
        print(f'Schedule already exists, updating: {name}')
        scheduler_client.update_schedule(**kwargs)
    except Exception as e:
        print(f'Failed to deploy schedule: {name} — {e}')


if __name__ == '__main__':
    schedules = load_schedules()
    boto3.setup_default_session(profile_name='AdministratorAccess-570204184505')
    client = boto3.client('scheduler', region_name='us-east-2')
    for schedule in schedules:
        deploy_schedule(client, schedule)