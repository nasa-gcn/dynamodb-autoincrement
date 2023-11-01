import {
  ConditionalCheckFailedException,
  DynamoDB,
} from '@aws-sdk/client-dynamodb'
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb'
import type { DynamoDBAutoIncrementProps } from '.'
import { DynamoDBAutoIncrement } from '.'

let doc: DynamoDBDocument
let autoincrement: DynamoDBAutoIncrement
let autoincrementDangerously: DynamoDBAutoIncrement
const N = 20

beforeAll(async () => {
  doc = DynamoDBDocument.from(
    new DynamoDB({
      credentials: {
        accessKeyId: 'fakeMyKeyId',
        secretAccessKey: 'fakeSecretAccessKey',
      },
      endpoint: 'http://localhost:8000',
      region: 'local-env',
    })
  )
  const options: DynamoDBAutoIncrementProps = {
    doc,
    counterTableName: 'autoincrement',
    counterTableKey: { tableName: 'widgets' },
    tableName: 'widgets',
    attributeName: 'widgetID',
    initialValue: 1,
  }
  autoincrement = new DynamoDBAutoIncrement(options)
  autoincrementDangerously = new DynamoDBAutoIncrement({
    ...options,
    dangerously: true,
  })
})

afterEach(async () => {
  // Delete all items of all tables
  await Promise.all(
    [
      { TableName: 'autoincrement', KeyAttributeName: 'tableName' },
      { TableName: 'widgets', KeyAttributeName: 'widgetID' },
    ].map(
      async ({ TableName, KeyAttributeName }) =>
        await Promise.all(
          ((await doc.scan({ TableName })).Items ?? []).map(
            async ({ [KeyAttributeName]: KeyValue }) =>
              await doc.delete({
                TableName,
                Key: { [KeyAttributeName]: KeyValue },
              })
          )
        )
    )
  )
})

describe('dynamoDBAutoIncrement', () => {
  test.each([undefined, 1, 2, 3])(
    'creates a new item with the correct ID when the old ID was %o',
    async (lastID) => {
      let nextID: number
      if (lastID === undefined) {
        nextID = 1
      } else {
        await doc.put({
          TableName: 'autoincrement',
          Item: { tableName: 'widgets', widgetID: lastID },
        })
        nextID = lastID + 1
      }

      const result = await autoincrement.put({ widgetName: 'runcible spoon' })
      expect(result).toEqual(nextID)

      const [widgetItems, autoincrementItems] = await Promise.all(
        ['widgets', 'autoincrement'].map(
          async (TableName) => (await doc.scan({ TableName })).Items
        )
      )

      expect(widgetItems).toEqual([
        { widgetID: nextID, widgetName: 'runcible spoon' },
      ])
      expect(autoincrementItems).toEqual([
        {
          tableName: 'widgets',
          widgetID: nextID,
        },
      ])
    }
  )

  test('correctly handles a large number of parallel puts', async () => {
    const ids = Array.from(Array(N).keys()).map((i) => i + 1)
    const result = await Promise.all(ids.map(() => autoincrement.put({})))
    expect(result.sort()).toEqual(ids.sort())
  })

  test('raises an error for unhandled DynamoDB exceptions', async () => {
    await expect(
      async () =>
        await autoincrement.put({
          widgetName: 'runcible spoon',
          description: 'Hello world! '.repeat(32000),
        })
    ).rejects.toThrow('Item size has exceeded the maximum allowed size')
  })
})

describe('dynamoDBAutoIncrement dangerously', () => {
  test('correctly handles a large number of serial puts', async () => {
    const ids = Array.from(Array(N).keys()).map((i) => i + 1)
    const result: number[] = []
    for (const item of ids) {
      result.push(await autoincrementDangerously.put({ widgetName: item }))
    }
    expect(result.sort()).toEqual(ids.sort())
  })

  test('fails on a large number of parallel puts', async () => {
    const ids = Array.from(Array(N).keys()).map((i) => i + 1)
    await expect(
      async () =>
        await Promise.all(ids.map(() => autoincrementDangerously.put({})))
    ).rejects.toThrow(ConditionalCheckFailedException)
  })
})
