import {
  ConditionalCheckFailedException,
  DescribeTableCommand,
  DynamoDBClient,
} from '@aws-sdk/client-dynamodb'
import {
  BatchWriteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  paginateScan,
} from '@aws-sdk/lib-dynamodb'
import type { DynamoDBAutoIncrementProps } from '.'
import { DynamoDBAutoIncrement, DynamoDBHistoryAutoIncrement } from '.'

let doc: DynamoDBDocumentClient
let autoincrement: DynamoDBAutoIncrement
let autoincrementVersion: DynamoDBHistoryAutoIncrement
let autoincrementDangerously: DynamoDBAutoIncrement
const N = 20

beforeAll(async () => {
  doc = DynamoDBDocumentClient.from(
    new DynamoDBClient({
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
  const versioningOptions: DynamoDBAutoIncrementProps = {
    doc,
    counterTableName: 'widgets',
    counterTableKey: {
      widgetID: 1,
    },
    attributeName: 'version',
    tableName: 'widgetHistory',
    initialValue: 1,
  }
  autoincrementVersion = new DynamoDBHistoryAutoIncrement(versioningOptions)
  autoincrementDangerously = new DynamoDBAutoIncrement({
    ...options,
    dangerously: true,
  })
})

function defined<T>(item: T | undefined): item is T {
  return item !== undefined
}

async function deleteAll(TableName: string) {
  const keyAttributeNames = (
    await doc.send(new DescribeTableCommand({ TableName }))
  ).Table?.KeySchema?.map(({ AttributeName }) => AttributeName).filter(defined)

  const pages = paginateScan(
    { client: doc, pageSize: 25 },
    { TableName, AttributesToGet: keyAttributeNames }
  )
  for await (const { Items } of pages) {
    if (Items?.length)
      await doc.send(
        new BatchWriteCommand({
          RequestItems: {
            [TableName]: Items.map((Key) => ({ DeleteRequest: { Key } })),
          },
        })
      )
  }
}

afterEach(async () => {
  // Delete all items of all tables
  await Promise.all(
    ['autoincrement', 'widgets', 'widgetHistory'].map(deleteAll)
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
        await doc.send(
          new PutCommand({
            TableName: 'autoincrement',
            Item: { tableName: 'widgets', widgetID: lastID },
          })
        )
        nextID = lastID + 1
      }

      const result = await autoincrement.put({ widgetName: 'runcible spoon' })
      expect(result).toEqual(nextID)

      const [widgetItems, autoincrementItems] = await Promise.all(
        ['widgets', 'autoincrement'].map(
          async (TableName) =>
            (await doc.send(new ScanCommand({ TableName }))).Items
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

describe('autoincrementVersion', () => {
  test('increments version on put when attributeName field is not defined on item', async () => {
    // Insert initial table item
    const widgetID = 1
    await doc.send(
      new PutCommand({
        TableName: 'widgets',
        Item: {
          widgetID,
          name: 'Handy Widget',
          description: 'Does something',
        },
      })
    )

    // Create new version
    const newVersion = await autoincrementVersion.put({
      name: 'Handy Widget',
      description: 'Does Everything!',
    })
    expect(newVersion).toBe(2)

    const historyItems = (
      await doc.send(
        new QueryCommand({
          TableName: 'widgetHistory',
          KeyConditionExpression: 'widgetID = :widgetID',
          ExpressionAttributeValues: {
            ':widgetID': widgetID,
          },
        })
      )
    ).Items

    expect(historyItems?.length).toBe(1)
  })

  test('increments version on put when attributeName field is defined on item', async () => {
    // Insert initial table item
    const widgetID = 1
    const initialItem = {
      widgetID,
      name: 'Handy Widget',
      description: 'Does something',
      version: 1,
    }
    await doc.send(
      new PutCommand({
        TableName: 'widgets',
        Item: initialItem,
      })
    )

    // Create new version
    const newVersion = await autoincrementVersion.put({
      name: 'Handy Widget',
      description: 'Does Everything!',
    })
    expect(newVersion).toBe(2)

    const historyItems = (
      await doc.send(
        new QueryCommand({
          TableName: 'widgetHistory',
          KeyConditionExpression: 'widgetID = :widgetID',
          ExpressionAttributeValues: {
            ':widgetID': widgetID,
          },
        })
      )
    ).Items

    expect(historyItems?.length).toBe(1)
  })

  test('increments version correctly if tracked field is included in the item on update', async () => {
    // Insert initial table item
    const widgetID = 1
    const initialItem = {
      widgetID,
      name: 'Handy Widget',
      description: 'Does something',
      version: 1,
    }
    await doc.send(
      new PutCommand({
        TableName: 'widgets',
        Item: initialItem,
      })
    )

    // Create new version
    const newVersion = await autoincrementVersion.put({
      name: 'Handy Widget',
      description: 'Does Everything!',
      version: 3,
    })
    expect(newVersion).toBe(2)
    const latestItem = (
      await doc.send(
        new GetCommand({
          TableName: 'widgets',
          Key: { widgetID },
        })
      )
    ).Item
    expect(latestItem).toStrictEqual({
      widgetID,
      name: 'Handy Widget',
      description: 'Does Everything!',
      version: 2,
    })
  })

  test('correctly handles a large number of parallel puts', async () => {
    const versions = Array.from(Array(N).keys()).map((i) => i + 2)
    await doc.send(
      new PutCommand({
        TableName: 'widgets',
        Item: {
          widgetID: 1,
          name: 'Handy Widget',
          description: 'Does something',
        },
      })
    )
    const result = await Promise.all(
      versions.map(() => autoincrementVersion.put({}))
    )
    expect(result.sort()).toEqual(versions.sort())
  })
})
