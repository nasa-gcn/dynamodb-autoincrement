/**
 * @type {import('@shelf/jest-dynamodb/lib').Config}')}
 */
const config = {
  tables: [
    {
      AttributeDefinitions: [
        { AttributeName: 'tableName', AttributeType: 'S' },
      ],
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: [{ AttributeName: 'tableName', KeyType: 'HASH' }],
      TableName: 'autoincrement',
    },
    {
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [{ AttributeName: 'widgetID', AttributeType: 'N' }],
      KeySchema: [{ AttributeName: 'widgetID', KeyType: 'HASH' }],
      TableName: 'widgets',
    },
    {
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [
        { AttributeName: 'widgetID', AttributeType: 'N' },
        { AttributeName: 'version', AttributeType: 'N' },
      ],
      KeySchema: [
        { AttributeName: 'widgetID', KeyType: 'HASH' },
        { AttributeName: 'version', KeyType: 'RANGE' },
      ],
      TableName: 'widgetHistory',
    },
  ],
  installerConfig: {
    installPath: './dynamodb_local_latest',
  },
}

module.exports = config
