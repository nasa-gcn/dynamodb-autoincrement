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
  counterTableAttributeName: string

  /** the name of the table in which to store items */
  tableName: string

  /** the name of the attribute used as the auto-incrementing partition key in the table in which to store items */
  tableAttributeName: string

  /** the initial value of the counter */
  initialValue: number

  /** if true, then do not perform any locking (suitable only for testing) */
  dangerously?: boolean
}

/**
 * Update an auto-incrementing partition key in DynamoDB.
 *
 * @example
 * ```
 * import { DynamoDB } from '@aws-sdk/client-dynamodb'
 * import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb'
 *
 * const client = new DynamoDB({})
 * const doc = DynamoDBDocument.from(client)
 *
 * const autoIncrement = dynamoDBAutoIncrement({
 *   doc,
 *   counterTableName: 'autoincrementHelper',
 *   counterTableKey: { autoincrementHelperForTable: 'widgets' },
 *   counterTableAttributeName: 'widgetIDCounter',
 *   tableName: 'widgets',
 *   tableAttributeName: 'widgetID',
 *   initialValue: 0,
 * })
 *
 * const lastWidgetID = await autoIncrement({
 *   widgetName: 'runcible spoon',
 *   costDollars: 99.99,
 * })
 * ```
 */
export class DynamoDBAutoIncrement {
  constructor(readonly props: DynamoDBAutoIncrementProps) {}

  async #getLast(): Promise<number | undefined> {
    return (
      (
        await this.props.doc.get({
          AttributesToGet: [this.props.counterTableAttributeName],
          Key: this.props.counterTableKey,
          TableName: this.props.counterTableName,
        })
      ).Item?.[this.props.counterTableAttributeName] ?? undefined
    )
  }

  async put(item: Record<string, NativeAttributeValue>) {
    for (;;) {
      const counter = await this.#getLast()

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
            '#counter': this.props.counterTableAttributeName,
          },
          ExpressionAttributeValues,
          Item: {
            ...this.props.counterTableKey,
            [this.props.counterTableAttributeName]: nextCounter,
          },
          TableName: this.props.counterTableName,
        },
        {
          ConditionExpression: 'attribute_not_exists(#counter)',
          ExpressionAttributeNames: {
            '#counter': this.props.tableAttributeName,
          },
          Item: { [this.props.tableAttributeName]: nextCounter, ...item },
          TableName: this.props.tableName,
        },
      ]

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
