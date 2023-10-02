import { existsSync, mkdirSync } from 'fs'
import { execSync } from 'child_process'

const dynamodbDir = 'dynamodb_local_latest'
const dynamodbArchiveURL =
  'https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz'

// Check if the dynamodbDir directory exists
if (!existsSync(dynamodbDir)) {
  // If it doesn't exist, create it
  mkdirSync(dynamodbDir, { recursive: true })

  // Download and extract DynamoDB Local
  try {
    console.log('Downloading DynamoDB Local...')
    execSync(`curl -L ${dynamodbArchiveURL} | tar -xz -C ${dynamodbDir}`, {
      stdio: 'inherit',
    })
    console.log('DynamoDB Local setup completed successfully.')
  } catch (error) {
    console.error('Error setting up DynamoDB Local:', error.message)
    process.exit(1)
  }
}
