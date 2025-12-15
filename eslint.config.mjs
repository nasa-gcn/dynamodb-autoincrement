import { defineConfig } from 'eslint/config'
import eslint from '@eslint/js'
import tseslint from 'typescript-eslint'
import eslintConfigPrettier from 'eslint-config-prettier/flat'
import pluginJest from 'eslint-plugin-jest'
import eslintConfigGitignore from '@nasa-gcn/eslint-config-gitignore'

export default defineConfig(
  eslintConfigGitignore,
  eslint.configs.recommended,
  eslintConfigPrettier,
  { files: ['*.js'], languageOptions: { sourceType: 'commonjs' } },
  { files: ['*.ts'], extends: [tseslint.configs.recommended] },
  {
    files: ['test.ts'],
    plugins: ['jest'],
    extends: [pluginJest.configs['flat/recommended']],
  }
)
