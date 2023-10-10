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
let autoincrementVersion: DynamoDBAutoIncrement
let autoincrementVersionDangerously: DynamoDBAutoIncrement

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
    counterTableAttributeName: 'counter',
    tableName: 'widgets',
    tableAttributeName: 'widgetID',
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
    ]
      .map(
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
      .concat(
        ...[
          {
            TableName: 'widgetHistory',
            PartitionKeyAttributeName: 'widgetID',
            SortKeyAttributeName: 'version',
          },
        ].map(
          async ({
            TableName,
            PartitionKeyAttributeName,
            SortKeyAttributeName,
          }) =>
            await Promise.all(
              ((await doc.scan({ TableName })).Items ?? []).map(
                async ({
                  [PartitionKeyAttributeName]: KeyValue,
                  [SortKeyAttributeName]: SortKeyValue,
                }) =>
                  await doc.delete({
                    TableName,
                    Key: {
                      [PartitionKeyAttributeName]: KeyValue,
                      [SortKeyAttributeName]: SortKeyValue,
                    },
                  })
              )
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
          Item: { tableName: 'widgets', counter: lastID },
        })
        nextID = lastID + 1
      }

      const result = await autoincrement.put({ widgetName: 'runcible spoon' })
      expect(result).toEqual(nextID)

      expect(await autoincrement.getLast()).toEqual(nextID)

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
          counter: nextID,
        },
      ])
    }
  )

  test('correctly handles a large number of parallel puts', async () => {
    const ids = Array.from(Array(N).keys()).map((i) => i + 1)
    const result = await Promise.all(ids.map(() => autoincrement.put({})))
    expect(result.sort()).toEqual(ids.sort())
  })

  test('increments version', async () => {
    const widgetID = 1
    const item = { description: 'widget A' }

    autoincrementVersion = new DynamoDBAutoIncrement({
      doc, // DDB document client instance
      counterTableCopyItem: true, //
      counterTableName: 'widgetHistory',
      counterTableKey: { widgetID }, // circularId for the Circular that we are editing
      tableName: 'widgets',
      tableAttributeName: 'version',
      initialValue: 1,
    })

    // Insert 1st item into table
    const insertResult = await autoincrement.put(item)
    expect(insertResult).toBe(widgetID)

    // Verify getLast matches latest insertResult
    const latestInsertResult = await autoincrement.getLast()
    expect(latestInsertResult).toBe(widgetID) // widgetID = 1

    // item inserted with autoincrement, version should be undefined
    const latestItem = await autoincrementVersion.getLastItem()
    expect(latestItem).toStrictEqual({ ...item, widgetID })

    // Update version in widgets, latestItem.verison is undefined,
    // add new row to widgetHistory
    const newWidgetVersion = await autoincrementVersion.put({
      widgetID,
      description: 'widget A updated',
    })
    expect(newWidgetVersion).toBe(2)
    const newWidget = await autoincrementVersion.getLastItem()
    expect(newWidget).toStrictEqual({
      widgetID,
      description: 'widget A updated',
      version: 2,
    })

    // Update version in widgets agian, latestItem.version should be 2
    const newVersion = {
      widgetID,
      description: 'widget A V3',
    }
    const widgetV3Version = await autoincrementVersion.put(newVersion)
    expect(widgetV3Version).toBe(3)
    const widgetV3 = await autoincrementVersion.getLastItem()
    expect(widgetV3).toStrictEqual({
      ...newVersion,
      version: 3,
    })

    const widgetHistory = await doc.query({
      TableName: autoincrementVersion.props.counterTableName,
      KeyConditionExpression: 'widgetID = :widgetID',
      ExpressionAttributeValues: {
        ':widgetID': 1,
      },
    })

    expect(widgetHistory.Count).toBe(2)
    if (!widgetHistory.Items) return false
    // count being > 0 means Items should not be empty
    expect(widgetHistory.Items[0]).toStrictEqual({
      ...item,
      widgetID,
      version: 1,
    })
  })
  test('increments version dangerously', async () => {
    const widgetID = 1
    const item = { description: 'widget A dangerous initial' }

    autoincrementVersionDangerously = new DynamoDBAutoIncrement({
      doc, // DDB document client instance
      counterTableCopyItem: true, //
      counterTableName: 'widgetHistory',
      counterTableKey: { widgetID }, // circularId for the Circular that we are editing
      tableName: 'widgets',
      tableAttributeName: 'version',
      initialValue: 1,
      dangerously: true,
    })

    // Insert 1st item into table
    const insertResult = await autoincrement.put(item)
    expect(insertResult).toBe(widgetID)

    // Verify getLast matches latest insertResult
    const latestInsertResult = await autoincrement.getLast()
    expect(latestInsertResult).toBe(widgetID) // widgetID = 1

    // item inserted with autoincrement, version should be undefined
    const latestItem = await autoincrementVersionDangerously.getLastItem()
    expect(latestItem).toStrictEqual({ ...item, widgetID })

    // Update version in widgets, latestItem.verison is undefined,
    // add new row to widgetHistory
    const newWidgetVersion = await autoincrementVersionDangerously.put({
      widgetID,
      description: 'widget A dangerous updated',
    })
    expect(newWidgetVersion).toBe(2)
    const newWidget = await autoincrementVersionDangerously.getLastItem()
    expect(newWidget).toStrictEqual({
      widgetID,
      description: 'widget A dangerous updated',
      version: 2,
    })

    // Update version in widgets agian, latestItem.version should be 2
    const newVersion = {
      widgetID,
      description: 'widget A V3',
    }
    const widgetV3Version =
      await autoincrementVersionDangerously.put(newVersion)
    expect(widgetV3Version).toBe(3)
    const widgetV3 = await autoincrementVersionDangerously.getLastItem()
    expect(widgetV3).toStrictEqual({
      ...newVersion,
      version: 3,
    })

    const widgetHistory = await doc.query({
      TableName: autoincrementVersionDangerously.props.counterTableName,
      KeyConditionExpression: 'widgetID = :widgetID',
      ExpressionAttributeValues: {
        ':widgetID': 1,
      },
    })

    expect(widgetHistory.Count).toBe(2)
    if (!widgetHistory.Items) return false
    // count being > 0 means Items should not be empty
    expect(widgetHistory.Items[0]).toStrictEqual({
      ...item,
      widgetID,
      version: 1,
    })
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
