import { mkdir } from 'fs/promises'
import { existsSync } from 'fs'
import stream from 'node:stream'
import tar from 'tar'

const dynamodbDir = 'dynamodb_local_latest'
const dynamodbArchiveURL =
  'https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz'

// Check if the dynamodbDir directory exists
async function runSetup() {
  if (!existsSync(dynamodbDir)) {
    // If it doesn't exist, create it
    await mkdir(dynamodbDir, { recursive: true })

    stream.Readable.fromWeb((await fetch(dynamodbArchiveURL)).body).pipe(
      tar.x({
        C: dynamodbDir,
      })
    )
  }
}

runSetup()
