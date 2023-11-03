import {
  ConditionalCheckFailedException,
  DynamoDB,
} from '@aws-sdk/client-dynamodb'
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb'
import type { DynamoDBAutoIncrementProps } from '.'
import { DynamoDBAutoIncrement, DynamoDBHistoryAutoIncrement } from '.'

let doc: DynamoDBDocument
let autoincrement: DynamoDBAutoIncrement
let autoincrementVersion: DynamoDBHistoryAutoIncrement
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

afterEach(async () => {
  // Delete all items of all tables
  await Promise.all(
    [
      { TableName: 'autoincrement', keyAttributeNames: ['tableName'] },
      { TableName: 'widgets', keyAttributeNames: ['widgetID'] },
      {
        TableName: 'widgetHistory',
        keyAttributeNames: ['widgetID', 'version'],
      },
    ].map(
      async ({ TableName, keyAttributeNames }) =>
        await Promise.all(
          ((await doc.scan({ TableName })).Items ?? []).map(
            async (item) =>
              await doc.delete({
                TableName,
                Key: Object.fromEntries(
                  keyAttributeNames.map((key) => [key, item[key]])
                ),
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
    await doc.put({
      TableName: 'widgets',
      Item: {
        widgetID,
        name: 'Handy Widget',
        description: 'Does something',
      },
    })

    // Create new version
    const newVersion = await autoincrementVersion.put({
      name: 'Handy Widget',
      description: 'Does Everything!',
    })
    expect(newVersion).toBe(2)

    const historyItems = (
      await doc.query({
        TableName: 'widgetHistory',
        KeyConditionExpression: 'widgetID = :widgetID',
        ExpressionAttributeValues: {
          ':widgetID': widgetID,
        },
      })
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
    await doc.put({
      TableName: 'widgets',
      Item: initialItem,
    })

    // Create new version
    const newVersion = await autoincrementVersion.put({
      name: 'Handy Widget',
      description: 'Does Everything!',
    })
    expect(newVersion).toBe(2)

    const historyItems = (
      await doc.query({
        TableName: 'widgetHistory',
        KeyConditionExpression: 'widgetID = :widgetID',
        ExpressionAttributeValues: {
          ':widgetID': widgetID,
        },
      })
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
    await doc.put({
      TableName: 'widgets',
      Item: initialItem,
    })

    // Create new version
    const newVersion = await autoincrementVersion.put({
      name: 'Handy Widget',
      description: 'Does Everything!',
      version: 3,
    })
    expect(newVersion).toBe(2)
    const latestItem = (
      await doc.get({
        TableName: 'widgets',
        Key: { widgetID },
      })
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
    await doc.put({
      TableName: 'widgets',
      Item: {
        widgetID: 1,
        name: 'Handy Widget',
        description: 'Does something',
      },
    })
    const result = await Promise.all(
      versions.map(() => autoincrementVersion.put({}))
    )
    expect(result.sort()).toEqual(versions.sort())
  })
})
