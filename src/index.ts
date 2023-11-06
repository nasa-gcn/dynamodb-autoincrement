import { TransactionCanceledException } from '@aws-sdk/client-dynamodb'
import type { DynamoDBDocument, PutCommandInput } from '@aws-sdk/lib-dynamodb'
import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb'

export interface DynamoDBAutoIncrementProps {
  /** a DynamoDB document client instance */
  doc: DynamoDBDocument

  /** the name of the table in which to store the last value of the counter */
  counterTableName: string

  /** the partition key in the table in which to store the last value of the counter */
  counterTableKey: Record<string, NativeAttributeValue>

  /** the name of the attribute in the table in which to store the last value of the counter */
  attributeName: string

  /** the name of the table in which to store items */
  tableName: string

  /** the initial value of the counter */
  initialValue: number

  /** if true, then do not perform any locking (suitable only for testing) */
  dangerously?: boolean
}

abstract class BaseDynamoDBAutoIncrement {
  constructor(readonly props: DynamoDBAutoIncrementProps) {}

  protected abstract next(
    item: Record<string, NativeAttributeValue>
  ): Promise<{ puts: PutCommandInput[]; nextCounter: number }>

  async put(item: Record<string, NativeAttributeValue>) {
    for (;;) {
      const { puts, nextCounter } = await this.next(item)

      if (this.props.dangerously) {
        await Promise.all(puts.map((obj) => this.props.doc.put(obj)))
      } else {
        try {
          await this.props.doc.transactWrite({
            TransactItems: puts.map((Put) => ({ Put })),
          })
        } catch (e) {
          if (e instanceof TransactionCanceledException) {
            continue
          } else {
            throw e
          }
        }
      }

      return nextCounter
    }
  }
}

/**
 * Update an auto-incrementing partition key in DynamoDB.
 *
 * @example
 * ```
 * import { DynamoDB } from '@aws-sdk/client-dynamodb'
 * import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb'
 * import { DynamoDBAutoIncrement } from '@nasa-gcn/dynamodb-autoincrement'
 *
 * const client = new DynamoDB({})
 * const doc = DynamoDBDocument.from(client)
 *
 * const autoIncrement = DynamoDBAutoIncrement({
 *   doc,
 *   counterTableName: 'autoincrementHelper',
 *   counterTableKey: { autoincrementHelperForTable: 'widgets' },
 *   counterTableAttributeName: 'widgetIDCounter',
 *   tableName: 'widgets',
 *   tableAttributeName: 'widgetID',
 *   initialValue: 0,
 * })
 *
 * const lastWidgetID = await autoIncrement.put({
 *   widgetName: 'runcible spoon',
 *   costDollars: 99.99,
 * })
 * ```
 */
export class DynamoDBAutoIncrement extends BaseDynamoDBAutoIncrement {
  protected async next(item: Record<string, NativeAttributeValue>) {
    const counter: number | undefined =
      (
        await this.props.doc.get({
          AttributesToGet: [this.props.attributeName],
          Key: this.props.counterTableKey,
          TableName: this.props.counterTableName,
        })
      ).Item?.[this.props.attributeName] ?? undefined

    let nextCounter, ConditionExpression, ExpressionAttributeValues
    if (counter === undefined) {
      nextCounter = this.props.initialValue
      ConditionExpression = 'attribute_not_exists(#counter)'
    } else {
      nextCounter = counter + 1
      ConditionExpression = '#counter = :counter'
      ExpressionAttributeValues = {
        ':counter': counter,
      }
    }

    const puts: PutCommandInput[] = [
      {
        ConditionExpression,
        ExpressionAttributeNames: {
          '#counter': this.props.attributeName,
        },
        ExpressionAttributeValues,
        Item: {
          ...this.props.counterTableKey,
          [this.props.attributeName]: nextCounter,
        },
        TableName: this.props.counterTableName,
      },
      {
        ConditionExpression: 'attribute_not_exists(#counter)',
        ExpressionAttributeNames: {
          '#counter': this.props.attributeName,
        },
        Item: { [this.props.attributeName]: nextCounter, ...item },
        TableName: this.props.tableName,
      },
    ]

    return { puts, nextCounter }
  }
}

/**
 * Update a history table with an auto-incrementing attribute value in DynamoDB
 *
 * @example
 * ```
 * import { DynamoDB } from '@aws-sdk/client-dynamodb'
 * import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb'
 * import { DynamoDBHistoryAutoIncrement } from '@nasa-gcn/dynamodb-autoincrement'
 *
 * const client = new DynamoDB({})
 * const doc = DynamoDBDocument.from(client)
 *
 * const autoIncrementHistory = DynamoDBHistoryAutoIncrement({
 *   doc,
 *   counterTableName: 'widgets', // The table storing the current item
 *   counterTableKey: {
 *     widgetID: 42 // ID of the item to be updated
 *   },
 *   attributeName: 'version',
 *   tableName: 'widgetsHistory', // The table storing the history of items in
 *   initialValue: 1,
 * })
 *
 * const latestVersionNumber = await autoIncrementHistory.put({
 *   widgetName: 'A new name for this item',
 *   costDollars: 199.99,
 * })
 * ```
 */
export class DynamoDBHistoryAutoIncrement extends BaseDynamoDBAutoIncrement {
  protected async next(item: Record<string, NativeAttributeValue>) {
    let nextCounter

    const existingItem = (
      await this.props.doc.get({
        TableName: this.props.counterTableName,
        Key: this.props.counterTableKey,
      })
    ).Item

    let counter: number | undefined = existingItem?.[this.props.attributeName]

    if (counter === undefined) {
      nextCounter = this.props.initialValue

      // Existing item didn't have a version, so give it one
      if (existingItem) {
        existingItem[this.props.attributeName] = counter = nextCounter
        nextCounter += 1
      }
    } else {
      nextCounter = counter + 1
    }

    const puts: PutCommandInput[] = [
      {
        ConditionExpression: 'attribute_not_exists(#counter)',
        ExpressionAttributeNames: {
          '#counter': this.props.attributeName,
        },
        Item: existingItem,
        TableName: this.props.tableName,
      },
    ]

    if (existingItem) {
      puts.push({
        ConditionExpression:
          'attribute_not_exists(#counter) OR #counter = :counter',
        ExpressionAttributeNames: {
          '#counter': this.props.attributeName,
        },
        ExpressionAttributeValues: {
          ':counter': counter,
        },
        Item: {
          ...item,
          ...this.props.counterTableKey,
          [this.props.attributeName]: nextCounter,
        },
        TableName: this.props.counterTableName,
      })
    }

    return { puts, nextCounter }
  }
}
