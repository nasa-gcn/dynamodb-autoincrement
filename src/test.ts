import {
  ConditionalCheckFailedException,
  DynamoDB,
} from '@aws-sdk/client-dynamodb'
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb'
import { DynamoDBAutoIncrementProps } from '.'
import { DynamoDBAutoIncrement } from '.'

let doc: DynamoDBDocument
let autoincrement: DynamoDBAutoIncrement
let autoincrementDangerously: DynamoDBAutoIncrement

let autoincrementPlusField: DynamoDBAutoIncrement
let autoincrementPlusFieldDangerously: DynamoDBAutoIncrement

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

  const fieldOptions = {
    ...options,
    counterTableKey: {
      tableName: 'widgets',
    },
    secondaryIncrementAttributeName: 'version',
    secondaryIncrementTableName: 'autoincrementField',
    secondaryIncrementItemPrimaryKey: 'widgetID',
    secondaryIncrementDefaultValue: 1,
  }
  autoincrement = new DynamoDBAutoIncrement(options)
  autoincrementPlusField = new DynamoDBAutoIncrement(fieldOptions)
  autoincrementDangerously = new DynamoDBAutoIncrement({
    ...options,
    dangerously: true,
  })

  autoincrementPlusFieldDangerously = new DynamoDBAutoIncrement({
    ...options,
    secondaryIncrementAttributeName: 'version',
    secondaryIncrementTableName: 'autoincrementField',
    secondaryIncrementItemPrimaryKey: 'widgetID',
    secondaryIncrementDefaultValue: 1,
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
            TableName: 'autoincrementField',
            PartitionKeyAttributeName: 'tableName',
            SortKeyAttributeName: 'tableItemPartitionKey',
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

describe('DynamoDBFieldAutoIncrement', () => {
  test.each([1, 2])(
    'gets the version for a widget with ID: %o',
    async (widgetID) => {
      // Add new rows to autoincrementField
      await doc.put({
        TableName: 'autoincrementField',
        Item: {
          tableName: 'widgets',
          tableItemPartitionKey: widgetID.toString(),
          version: 1,
        },
      })

      // Check version == 1
      expect(await autoincrementPlusField.getLastPerItem({ widgetID })).toEqual(
        1
      )
    }
  )

  test('returns undefined when the field is missing', async () => {
    const result = await autoincrementPlusField.getLastPerItem({
      widgetID: 3,
    })
    expect(result).toBeUndefined()
  })

  test('inserts a new entry with tracked field set to 1', async () => {
    // Insert a new row into the widget table
    const result = await autoincrementPlusField.put({
      widgetName: 'useful widget',
      version: autoincrementPlusField.props.secondaryIncrementDefaultValue,
    })
    const version = await autoincrementPlusField.getLastPerItem({
      widgetID: result,
    })
    expect(version).toBe(1)
  })

  test('updates the specified increment field on update', async () => {
    const result = await autoincrementPlusField.put({
      widgetName: 'new widget',
      version: autoincrementPlusField.props.secondaryIncrementDefaultValue,
    })
    const version = await autoincrementPlusField.getLastPerItem({
      widgetID: result,
    })
    expect(version).toBe(1)

    if (!version) return false

    const updatedVersion = await autoincrementPlusField.update({
      widgetID: result,
      widgetName: 'better widget',
    })
    const updatedWidget = (
      await doc.get({
        TableName: 'widgets',
        Key: { widgetID: result },
      })
    ).Item
    expect(updatedWidget).toBeDefined()
    expect(updatedWidget?.widgetName).toBe('better widget')
    expect(updatedVersion).toBe(2)
  })

  test('throws an error when the partition key is missing from update', async () => {
    const result = await autoincrementPlusField.put({
      widgetName: 'new widget',
      version: autoincrementPlusField.props.secondaryIncrementDefaultValue,
    })
    const version = await autoincrementPlusField.getLastPerItem({
      widgetID: result,
    })
    expect(version).toBe(1)

    if (!version) return false

    const updatedVersion = async () => {
      await autoincrementPlusField.update({
        // widgetID:result, intentionally commented out to show case for missing ID
        widgetName: 'better widget',
      })
    }
    await expect(updatedVersion()).rejects.toThrow(TypeError)
  })
})

describe('DynamoDBFieldAutoIncrement dangerously', () => {
  test('inserts a new entry with tracked field set to 1', async () => {
    // Insert a new row into the widget table
    const result = await autoincrementPlusFieldDangerously.put({
      widgetName: 'useful widget',
      version:
        autoincrementPlusFieldDangerously.props.secondaryIncrementDefaultValue,
    })
    const version = await autoincrementPlusFieldDangerously.getLastPerItem({
      widgetID: result,
    })
    expect(version).toBe(1)
  })

  test('updates the specified increment field on update', async () => {
    const result = await autoincrementPlusFieldDangerously.put({
      widgetName: 'new widget',
      version:
        autoincrementPlusFieldDangerously.props.secondaryIncrementDefaultValue,
    })
    const version = await autoincrementPlusFieldDangerously.getLastPerItem({
      widgetID: result,
    })
    expect(version).toBe(1)

    if (!version) return false

    const updatedVersion = await autoincrementPlusFieldDangerously.update({
      widgetID: result,
      widgetName: 'better widget',
      bonusProp: 123456,
    })
    expect(updatedVersion).toBe(2)
    const updatedWidget = (
      await doc.get({
        TableName: 'widgets',
        Key: { widgetID: result },
      })
    ).Item
    expect(updatedWidget).toBeDefined()
    expect(updatedWidget?.widgetName).toBe('better widget')
    expect(updatedWidget?.bonusProp).toBe(123456)
  })
})
