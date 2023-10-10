import { TransactionCanceledException } from '@aws-sdk/client-dynamodb'
import type {
  DynamoDBDocument,
  PutCommandInput,
  UpdateCommandInput,
} from '@aws-sdk/lib-dynamodb'
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

  async getLast(): Promise<number | undefined> {
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
      const counter = await this.getLast()

      let nextCounter: number
      let Update: UpdateCommandInput & { UpdateExpression: string }

      if (counter === undefined) {
        nextCounter = this.props.initialValue
        Update = {
          ConditionExpression: 'attribute_not_exists(#counter)',
          ExpressionAttributeNames: {
            '#counter': this.props.counterTableAttributeName,
          },
          ExpressionAttributeValues: {
            ':nextCounter': nextCounter,
          },
          Key: this.props.counterTableKey,
          TableName: this.props.counterTableName,
          UpdateExpression: 'SET #counter = :nextCounter',
        }
      } else {
        nextCounter = counter + 1
        Update = {
          ConditionExpression: '#counter = :counter',
          ExpressionAttributeNames: {
            '#counter': this.props.counterTableAttributeName,
          },
          ExpressionAttributeValues: {
            ':counter': counter,
            ':nextCounter': nextCounter,
          },
          Key: this.props.counterTableKey,
          TableName: this.props.counterTableName,
          UpdateExpression: 'SET #counter = :nextCounter',
        }
      }

      const Put: PutCommandInput = {
        ConditionExpression: 'attribute_not_exists(#counter)',
        ExpressionAttributeNames: { '#counter': this.props.tableAttributeName },
        Item: { '#counter': nextCounter, ...item },
        TableName: this.props.tableName,
      }

      if (this.props.dangerously) {
        await Promise.all([
          this.props.doc.update(Update),
          this.props.doc.put(Put),
        ])
      } else {
        try {
          await this.props.doc.transactWrite({
            TransactItems: [{ Update }, { Put }],
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
