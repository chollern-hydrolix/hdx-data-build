# Set up a second Claude Code account

1. **Pick a name** for the second profile (e.g., `work`).

2. **Create its config dir**:
   ```bash
   mkdir -p ~/.claude-work
   ```

3. **Add an alias** to `~/.zshrc`:
   ```bash
   alias claude-work='CLAUDE_CONFIG_DIR=~/.claude-work claude'
   ```
   Then reload:
   ```bash
   source ~/.zshrc
   ```

4. **Launch and log in**:
   ```bash
   claude-work
   ```
   Inside the session, run `/login` and authenticate as the second account.

5. **Use your default account** with plain `claude`, and the second with `claude-work`. Repeat steps 2–4 to add more profiles.