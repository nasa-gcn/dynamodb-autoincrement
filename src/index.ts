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

  /** the name of the table within which the secondaryIncrementField incrementation will happen */
  secondaryIncrementTableName?: string

  /** the value of the given field will be set as the initial value for the field-level incrementation */
  secondaryIncrementAttributeName?: string

  /** name of the Partition Key field for the source item */
  secondaryIncrementItemPrimaryKey?: string

  /**  */
  secondaryIncrementDefaultValue?: number
}

export interface CounterTableKey {
  tableName: string
}

export interface CompoundCounterTableKey extends CounterTableKey {
  tableItemPartitionKey: string
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
 * const lastWidgetID = await autoIncrement.put({
 *   widgetName: 'runcible spoon',
 *   costDollars: 99.99,
 * })
 * ```
 * 
 * @example - Incrementing Partition key and initialize a
 * ```
 * import { DynamoDB } from '@aws-sdk/client-dynamodb'
 * import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb'
 *
 * const client = new DynamoDB({})
 * const doc = DynamoDBDocument.from(client)
 *
 * const autoIncrement = new DynamoDBFieldAutoIncrement({
 *   doc,
 *   counterTableName: 'autoincrementHelper',
 *   counterTableKey: {
 *     tableName: 'widgets',
 *     tableItemPartitionKey: widgetID,
 *   },
 *   counterTableAttributeName: 'version',
 *   tableName: 'widgets',
 *   tableAttributeName: 'version',
 *   initialValue: 1,
 * })
 *
 * const lastWidgetID = await autoIncrement.put({
 *   widgetName: 'runcible spoon',
 *   costDollars: 99.99,
 * })
 * ```

 */
export class DynamoDBAutoIncrement {
  useSecondaryIndexing: boolean
  secondaryIncrementAttributeName: string
  secondaryIncrementTableName: string
  secondaryIncrementItemPrimaryKey: string
  secondaryIncrementDefaultValue: number
  constructor(readonly props: DynamoDBAutoIncrementProps) {
    this.useSecondaryIndexing =
      props.secondaryIncrementAttributeName != undefined &&
      props.secondaryIncrementDefaultValue != undefined &&
      props.secondaryIncrementItemPrimaryKey != undefined &&
      props.secondaryIncrementDefaultValue != undefined

    this.secondaryIncrementAttributeName =
      props.secondaryIncrementAttributeName ?? ''
    this.secondaryIncrementDefaultValue =
      props.secondaryIncrementDefaultValue ?? 1
    this.secondaryIncrementItemPrimaryKey =
      props.secondaryIncrementItemPrimaryKey ?? ''
    this.secondaryIncrementTableName = props.secondaryIncrementTableName ?? ''
  }

  /**
   * Gets the latest value of the incrementing partition key for
   * items in the table defined on `this.props.counterTableKey.tableName`
   * @returns
   */
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

  /**
   *  Gets the latest value of the tracked attribute for a provided item
   * @param item
   * @returns
   */
  async getLastPerItem(
    item: Record<string, NativeAttributeValue>
  ): Promise<number | undefined> {
    if (
      !this.props.secondaryIncrementItemPrimaryKey ||
      !this.props.secondaryIncrementAttributeName
    )
      throw new Error(
        'secondaryIncrementItemPrimaryKey and secondaryIncrementAttributeName are required for per-entry indexing'
      )
    console.log(JSON.stringify(item))
    const key = {
      tableName: this.props.counterTableKey.tableName,
      tableItemPartitionKey:
        item[this.props.secondaryIncrementItemPrimaryKey].toString(),
    }
    return (
      (
        await this.props.doc.get({
          AttributesToGet: [this.props.secondaryIncrementAttributeName],
          Key: key,
          TableName: this.props.secondaryIncrementTableName,
        })
      ).Item?.[this.props.secondaryIncrementAttributeName] ?? undefined
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

      const newItem = this.useSecondaryIndexing
        ? {
            ...item,
            [this.secondaryIncrementAttributeName]:
              this.props.secondaryIncrementDefaultValue,
          }
        : item

      const Put: PutCommandInput = {
        ConditionExpression: 'attribute_not_exists(#counter)',
        ExpressionAttributeNames: { '#counter': this.props.tableAttributeName },
        Item: { [this.props.tableAttributeName]: nextCounter, ...newItem },
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

      if (
        this.props.secondaryIncrementAttributeName &&
        this.props.secondaryIncrementTableName &&
        this.props.secondaryIncrementItemPrimaryKey
      ) {
        const secondaryIncrementItem = {
          tableName: this.props.tableName,
          tableItemPartitionKey: nextCounter.toString(),
          [this.props.secondaryIncrementAttributeName]:
            this.props.secondaryIncrementDefaultValue,
        }

        const SecondaryIncrementPut: PutCommandInput = {
          ConditionExpression:
            'attribute_not_exists(#pk) and attribute_not_exists(#sk)',
          ExpressionAttributeNames: {
            '#pk': this.props.tableName,
            '#sk': nextCounter.toString(),
          },
          Item: secondaryIncrementItem,
          TableName: this.props.secondaryIncrementTableName,
        }

        await this.props.doc.put(SecondaryIncrementPut)
      }

      return nextCounter
    }
  }

  /** Updates the tracked field for a given entry, if `getLastPerItem()` returns
   * undefined, the counter will default to the `secondaryIncrementDefaultValue`.
   *
   * Use the `put` method defined on the `DynamoDBAutoIncrement` class to initialize
   * new entries with a tracked value
   */
  async update(item: Record<string, NativeAttributeValue>) {
    if (this.verifySecondaryIncrementProps()) throw new Error()

    for (;;) {
      const counter =
        (await this.getLastPerItem(item)) ?? this.secondaryIncrementDefaultValue
      const nextCounter = counter + 1
      // const temp = this.props.counterTableKey as CompoundCounterTableKey

      const Update: UpdateCommandInput & { UpdateExpression: string } = {
        ConditionExpression: '#counter = :counter',
        ExpressionAttributeNames: {
          '#counter': this.secondaryIncrementAttributeName,
        },
        ExpressionAttributeValues: {
          ':counter': counter,
          ':nextCounter': nextCounter,
        },
        Key: {
          tableName: this.props.counterTableKey.tableName,
          tableItemPartitionKey:
            item[this.secondaryIncrementItemPrimaryKey].toString(),
        },
        TableName: this.secondaryIncrementTableName,
        UpdateExpression: 'SET #counter = :nextCounter',
      }

      if (this.props.dangerously) {
        await this.props.doc.update(Update)
      } else {
        try {
          await this.props.doc.transactWrite({
            TransactItems: [{ Update }],
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
  verifySecondaryIncrementProps(): boolean {
    return (
      !this.props.secondaryIncrementAttributeName ||
      !this.props.secondaryIncrementDefaultValue ||
      !this.props.secondaryIncrementItemPrimaryKey ||
      !this.props.secondaryIncrementTableName
    )
  }
}
