# Oracle to PostgreSQL Migration Tool

A high-performance, concurrent data migration tool built with TypeScript for migrating data from Oracle databases to PostgreSQL with advanced features like batch processing, custom transformations, and multiple pagination strategies.

## 🚀 Features

- **Concurrent Processing**: Migrate multiple tables and batches simultaneously
- **Smart Pagination**: Multiple strategies (ROWNUM, OFFSET, Cursor-based) with auto-detection
- **Custom Transformations**: Flexible data transformation with field mapping and custom logic
- **Checkpoint Recovery**: Resume interrupted migrations from where they left off
- **Progress Monitoring**: Real-time progress tracking and logging
- **Retry Logic**: Automatic retry with exponential backoff for failed operations
- **Memory Efficient**: Processes data in configurable batches to handle large datasets

## 📋 Prerequisites

- **Node.js**: >= 16.0.0
- **Oracle Instant Client**: Required for Oracle database connectivity
- **Database Access**: Read access to Oracle source database and write access to PostgreSQL target database

### Installing Oracle Instant Client

**Ubuntu/Debian:**

```bash
sudo apt-get install oracle-instantclient-basic
```

**macOS (with Homebrew):**

```bash
brew install instantclient-basic
```

**Windows/Manual Installation:**
Download from [Oracle's website](https://www.oracle.com/database/technologies/instant-client.html)

## 🛠 Installation

1. **Clone or download the project**

```bash
git clone <repository-url>
cd oracle-postgres-migration
```

2. **Install dependencies**

```bash
npm install
```

3. **Create project structure**

```bash
mkdir -p src/{types,config,utils,database,data,migration}
```

4. **Copy the source files** into their respective directories as shown in the project structure

## 📁 Project Structure

```
src/
├── types/
│   └── index.ts              # TypeScript interfaces and types
├── config/
│   └── index.ts              # Configuration management
├── utils/
│   ├── logger.ts             # Logging utilities
│   ├── helpers.ts            # Helper functions (retry, sleep)
│   └── semaphore.ts          # Concurrency control
├── database/
│   ├── oracle.ts             # Oracle database operations
│   ├── postgres.ts           # PostgreSQL database operations
│   ├── pagination.ts         # Pagination strategies
│   └── connections.ts        # Connection management
├── data/
│   ├── checkpoint.ts         # Progress checkpoint management
│   └── transform.ts          # Data transformation functions
├── migration/
│   ├── migrator.ts           # Basic migration logic
│   ├── concurrent-migrator.ts # Advanced concurrent migration
│   └── runner.ts             # Migration orchestration
└── index.ts                  # Main entry point
```

## ⚙️ Configuration

### Basic Configuration

```typescript
import { createConfig } from "config";

const config = createConfig({
  // Oracle database settings
  oracleHost: "oracle-server.example.com",
  oraclePort: 1521,
  oracleServiceName: "ORCL",
  oracleUser: "your_oracle_user",
  oraclePassword: "your_oracle_password",

  // PostgreSQL database settings
  postgresHost: "postgres-server.example.com",
  postgresPort: 5432,
  postgresDatabase: "your_postgres_db",
  postgresUser: "your_postgres_user",
  postgresPassword: "your_postgres_password",

  // Performance settings
  batchSize: 1000,
  maxConcurrentBatches: 4,
  maxConcurrentTables: 2,

  // Pagination strategy
  paginationStrategy: "cursor", // 'rownum', 'offset', or 'cursor'
  cursorColumn: "id",
});
```

### Environment Variables (Recommended)

Create a `.env` file:

```bash
ORACLE_HOST=your-oracle-server.com
ORACLE_USER=your_username
ORACLE_PASSWORD=your_password
POSTGRES_HOST=your-postgres-server.com
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```

## 🎯 Usage

### Simple Migration

```typescript
import { runMigration } from "migration/runner";
import { createFieldMapper } from "data/transform";

// Define transformations
const userTransform = createFieldMapper({
  USER_ID: "id",
  FULL_NAME: "name",
  EMAIL_ADDRESS: "email",
});

// Define migrations
const migrations = [
  {
    sourceQuery: "SELECT * FROM users ORDER BY user_id",
    targetTable: "users",
    transformFn: userTransform,
  },
];

// Run migration
await runMigration(config, migrations);
```

### Advanced Migration with Custom Transformations

```typescript
import { createCustomTransformer } from "data/transform";

const orderTransform = createCustomTransformer((row: any) => ({
  id: row.ORDER_ID,
  customer_id: row.CUSTOMER_ID,
  total: parseFloat(row.TOTAL_AMOUNT),
  status: row.ORDER_STATUS?.toLowerCase(),
  created_at: new Date(row.ORDER_DATE),
  // Computed fields
  is_large_order: parseFloat(row.TOTAL_AMOUNT) > 1000,
  formatted_total: `$${parseFloat(row.TOTAL_AMOUNT).toFixed(2)}`,
}));

const migrations = [
  {
    sourceQuery: "SELECT * FROM orders ORDER BY order_id",
    targetTable: "orders",
    transformFn: orderTransform,
    priority: 10, // High priority
    maxConcurrentBatches: 6, // Override global setting
    paginationStrategy: "cursor",
    cursorColumn: "order_id",
  },
];
```

## 🔄 Pagination Strategies

### 1. ROWNUM (Oracle Default)

- **Best for**: Small to medium tables (< 100K rows)
- **Pros**: Simple, reliable, works with any query
- **Cons**: Performance degrades with large offsets

```typescript
{
  paginationStrategy: "rownum";
}
```

### 2. OFFSET/LIMIT

- **Best for**: Medium tables with good indexing
- **Pros**: Standard SQL, portable across databases
- **Cons**: Can be slow with large offsets

```typescript
{
    paginationStrategy: 'offset',
    orderByClause: 'ORDER BY id ASC'
}
```

### 3. Cursor-Based (Recommended for Large Tables)

- **Best for**: Large tables (1M+ rows)
- **Pros**: Consistent performance, memory efficient
- **Cons**: Requires indexed column, more complex

```typescript
{
    paginationStrategy: 'cursor',
    cursorColumn: 'id',
    orderByClause: 'ORDER BY id ASC'
}
```

## 🎛 Performance Tuning

### High-Performance Setup

```typescript
const config = createConfig({
  batchSize: 5000, // Larger batches
  maxConcurrentBatches: 8, // More concurrent batches
  maxConcurrentTables: 3, // More concurrent tables
  maxConcurrentConnections: 15, // Larger connection pool
  paginationStrategy: "cursor", // Most efficient pagination
});
```

### Conservative Setup (Safe Mode)

```typescript
const config = createConfig({
  batchSize: 1000, // Smaller batches
  maxConcurrentBatches: 2, // Limited concurrency
  maxConcurrentTables: 1, // Sequential table processing
  paginationStrategy: "rownum", // Simple pagination
});
```

## 📊 Monitoring and Logging

The tool provides comprehensive logging and progress monitoring:

### Log Files

- `migration.log`: Detailed migration logs
- `migration_checkpoint.json`: Progress checkpoint for recovery

### Progress Monitoring

Real-time progress updates every 30 seconds:

```
Progress: 2/4 tables complete, 150000 total rows processed, 3 active batches, 2 queued batches
```

### Custom Progress Monitoring

```typescript
import { ConcurrentMigrator } from "migration/concurrent-migrator";

const migrator = new ConcurrentMigrator(connections, config, checkpoint);
const progress = await migrator.getProgress();
console.log(
  `Completed: ${progress.completedTables}/${progress.totalTables} tables`,
);
```

## 🔄 Recovery and Checkpoints

The migration tool automatically saves progress and can resume from interruptions:

### Automatic Recovery

- Progress is saved after each batch
- Restart the migration to resume from the last checkpoint
- Per-table progress tracking prevents data duplication

### Manual Checkpoint Management

```typescript
import { loadCheckpoint, saveCheckpoint } from "data/checkpoint";

// Load existing checkpoint
const checkpoint = await loadCheckpoint("migration_checkpoint.json");

// Save custom checkpoint
await saveCheckpoint("migration_checkpoint.json", lastId, totalProcessed);
```

## 🧪 Running Migrations

### Development Mode

```bash
npm run dev
```

### Production Mode

```bash
npm run migrate
```

### Sequential Migration (Safe Mode)

```bash
npm run migrate:sequential
```

### Build Only

```bash
npm run build
```

## 🛡 Error Handling

### Automatic Retry Logic

- Failed operations are automatically retried
- Configurable retry attempts and delays
- Exponential backoff prevents overwhelming servers

### Custom Error Handling

```typescript
try {
  await runMigration(config, migrations);
} catch (error) {
  console.error("Migration failed:", error.message);
  // Custom recovery logic here
}
```

## 🎨 Data Transformations

### Field Mapping

```typescript
const transform = createFieldMapper({
  ORACLE_FIELD: "postgres_field",
  OLD_NAME: "new_name",
});
```

### Custom Logic

```typescript
const transform = createCustomTransformer((row: any) => ({
  id: row.ID,
  name: row.FULL_NAME?.trim().toUpperCase(),
  email: row.EMAIL?.toLowerCase(),
  created_at: new Date(row.CREATED_DATE),
  // Computed fields
  display_name: `${row.FIRST_NAME} ${row.LAST_NAME}`,
  account_status: row.ACTIVE === 1 ? "active" : "inactive",
}));
```

### Built-in Transformations

```typescript
import { transforms } from "data/transform";

// Lowercase all field names
const simple = transforms.lowercaseFields;

// Map fields with type conversion
const mapped = transforms.mapAndConvert({
  USER_ID: "id",
  EMAIL_ADDR: "email",
});

// Add computed fields
const enhanced = transforms.withComputedFields({
  full_name: (row) => `${row.FIRST_NAME} ${row.LAST_NAME}`,
  is_admin: (row) => row.ROLE === "ADMIN",
});
```

## 🚨 Troubleshooting

### Common Issues

**Oracle Client not found**

```bash
# Install Oracle Instant Client
sudo apt-get install oracle-instantclient-basic
```

**Connection timeouts**

```typescript
// Increase timeout settings
postgres: {
    connectionTimeoutMillis: 30000,
    idleTimeoutMillis: 60000
}
```

**Memory issues with large datasets**

```typescript
// Reduce batch size
batchSize: 500;
```

**Slow performance**

```typescript
// Optimize settings
{
    batchSize: 2000,
    maxConcurrentBatches: 6,
    paginationStrategy: 'cursor'
}
```

### Debug Mode

Enable detailed logging by modifying the logger:

```typescript
// In utils/logger.ts
export const logDebug = (message: string) => log("debug", message);
```

## 📄 License

MIT License - see LICENSE file for details

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For issues and questions:

- Check the troubleshooting section
- Review the logs in `migration.log`
- Create an issue with detailed error information

---
