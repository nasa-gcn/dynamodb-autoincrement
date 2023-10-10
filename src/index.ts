import { TransactionCanceledException } from '@aws-sdk/client-dynamodb'
import type {
  DynamoDBDocument,
  PutCommandInput,
  UpdateCommandInput,
} from '@aws-sdk/lib-dynamodb'
import { NativeAttributeValue } from '@aws-sdk/util-dynamodb'

export interface DynamoDBAutoIncrementProps {
  /** a DynamoDB document client instance */
  doc: DynamoDBDocument

  /** whether to copy all of the attributes from the table to the counterTable */
  counterTableCopyItem?: boolean

  /** the name of the table in which to store the last value of the counter */
  counterTableName: string

  /** the partition key in the table in which to store the last value of the counter */
  counterTableKey: Record<string, NativeAttributeValue>

  /** the name of the attribute in the table in which to store the last value of the counter.
   * If undefined, defaults to tableAttributeName. */
  counterTableAttributeName?: string

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
  constructor(readonly props: DynamoDBAutoIncrementProps) {
    this.#attributeName =
      this.props.counterTableAttributeName ?? this.props.tableAttributeName
  }
  #attributeName: string
  async getLast(): Promise<number | undefined> {
    return (
      (
        await this.props.doc.get({
          AttributesToGet: [this.#attributeName],
          Key: this.props.counterTableKey,
          TableName: this.props.counterTableName,
        })
      ).Item?.[this.#attributeName] ?? undefined
    )
  }

  async getLastItem() {
    return (
      (
        await this.props.doc.get({
          TableName: this.props.tableName,
          Key: this.props.counterTableKey,
        })
      ).Item ?? undefined
    )
  }

  async put(item: Record<string, NativeAttributeValue>) {
    let nextCounter: number
    let Update: UpdateCommandInput & { UpdateExpression: string }

    if (this.props.counterTableCopyItem) {
      const ExpressionAttributeNames = this.#getExpressionAttributeNames(item)
      const ExpressionAttributeValues = this.#getExpressionAttributeValues(item)
      const UpdateExpression = this.#getUpdateExpression(item)
      for (;;) {
        const previousItem = await this.getLastItem()
        const counter = previousItem?.[this.#attributeName]

        if (counter === undefined) {
          nextCounter = this.props.initialValue + 1
          Update = {
            ConditionExpression: 'attribute_not_exists(#counter)',
            ExpressionAttributeNames: {
              ...ExpressionAttributeNames,
              '#counter': this.#attributeName,
            },
            ExpressionAttributeValues: {
              ...ExpressionAttributeValues,
              ':nextCounter': nextCounter,
            },
            Key: this.props.counterTableKey,
            TableName: this.props.tableName,
            UpdateExpression,
          }
        } else {
          nextCounter = counter + 1
          Update = {
            ConditionExpression: '#counter = :counter',
            ExpressionAttributeNames: {
              ...ExpressionAttributeNames,
              '#counter': this.#attributeName,
            },
            ExpressionAttributeValues: {
              ...ExpressionAttributeValues,
              ':counter': counter,
              ':nextCounter': nextCounter,
            },
            Key: this.props.counterTableKey,
            TableName: this.props.tableName,
            UpdateExpression,
          }
        }

        // PUT should be done to this.props.counterTableName
        const Put: PutCommandInput = {
          ConditionExpression: `${this.#getKeyConditionExpression(
            this.props.counterTableKey
          )} AND attribute_not_exists(#counter)`,
          ExpressionAttributeNames: {
            ...this.#getKeyExpressionAttributeNames(this.props.counterTableKey),
            '#counter': this.props.tableAttributeName,
          },
          Item: {
            ...previousItem,
            [this.props.tableAttributeName]: counter ?? 1,
          },
          TableName: this.props.counterTableName,
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
    } else {
      for (;;) {
        const counter = await this.getLast()

        if (counter === undefined) {
          nextCounter = this.props.initialValue
          Update = {
            ConditionExpression: 'attribute_not_exists(#counter)',
            ExpressionAttributeNames: {
              '#counter': this.#attributeName,
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
              '#counter': this.#attributeName,
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
          ExpressionAttributeNames: {
            '#counter': this.props.tableAttributeName,
          },
          Item: { [this.props.tableAttributeName]: nextCounter, ...item },
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

  #getKeyConditionExpression(item: Record<string, NativeAttributeValue>) {
    return Object.keys(item)
      .map((key) => `attribute_not_exists(#${key})`)
      .join(' AND ')
  }

  #getKeyExpressionAttributeNames(item: Record<string, NativeAttributeValue>) {
    const result: Record<string, string> = {}
    Object.keys(item).forEach((key) => {
      result[`#${key}`] = key
    })
    return result
  }

  #getExpressionAttributeValues(item: Record<string, NativeAttributeValue>) {
    const result: Record<string, NativeAttributeValue> = {}
    Object.keys(item)
      // Filter to remove the Partition Key from the ExpressionAttributeValues
      .filter((x) => !Object.keys(this.props.counterTableKey).includes(x))
      .forEach((key) => {
        result[`:${key}`] = item[key]
      })
    return result
  }

  #getExpressionAttributeNames(item: Record<string, NativeAttributeValue>) {
    const result: Record<string, string> = {}
    Object.keys(item)
      // Filter to remove the Partition Key from the ExpressionAttributeNames
      .filter((x) => !Object.keys(this.props.counterTableKey).includes(x))
      .forEach((key) => {
        result[`#${key}`] = key
      })
    return result
  }

  #getUpdateExpression(item: Record<string, NativeAttributeValue>): string {
    let result = 'SET #counter = :nextCounter'
    if (this.props.counterTableCopyItem) {
      // Update happens on this.props.tableName
      const keyArray = Object.keys(item)
        // Filter to remove the Partition Key from the UpdateExpression
        .filter((x) => !Object.keys(this.props.counterTableKey).includes(x))
        .map((key) => `#${key} = :${key}`)
      keyArray.splice(0, 0, 'SET #counter = :nextCounter')
      result = keyArray.join(', ')
    }
    return result
  }
}
