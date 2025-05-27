import oracledb from "oracledb";
import { OracleConfig, PaginationContext } from "types";
import { logInfo, logError } from "utils/logger";
import { buildPaginatedQuery, extractCursorValue } from "database/pagination";

export const connectToOracle = async (
  config: OracleConfig,
): Promise<oracledb.Connection> => {
  try {
    const connectionString = `${config.host}:${config.port}/${config.serviceName}`;

    const connection = await oracledb.getConnection({
      user: config.user,
      password: config.password,
      connectString: connectionString,
    });

    await logInfo("Connected to Oracle database");
    return connection;
  } catch (error) {
    await logError(`Failed to connect to Oracle: ${(error as Error).message}`);
    throw error;
  }
};

export const fetchOracleDataWithPagination = async (
  connection: oracledb.Connection,
  baseQuery: string,
  paginationContext: PaginationContext,
): Promise<{ rows: any[]; lastCursorValue?: any; hasMore: boolean }> => {
  try {
    const paginatedQuery = buildPaginatedQuery(baseQuery, paginationContext);

    await logInfo(
      `Executing paginated query with ${paginationContext.strategy} strategy`,
    );

    const result = await connection.execute(paginatedQuery, [], {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
      fetchArraySize: paginationContext.batchSize,
    });

    const rows = result.rows || [];

    // Extract cursor value for cursor-based pagination
    let lastCursorValue = undefined;
    if (
      paginationContext.strategy === "cursor" &&
      paginationContext.cursorColumn
    ) {
      lastCursorValue = extractCursorValue(
        rows,
        paginationContext.cursorColumn,
      );
    }

    // Determine if there are more rows
    const hasMore = rows.length === paginationContext.batchSize;

    return { rows, lastCursorValue, hasMore };
  } catch (error) {
    await logError(`Error fetching Oracle data: ${(error as Error).message}`);
    throw error;
  }
};

// Legacy function for backward compatibility
export const fetchOracleData = async (
  connection: oracledb.Connection,
  query: string,
  offset: number,
  batchSize: number,
): Promise<any[]> => {
  const context: PaginationContext = {
    strategy: "rownum",
    offset,
    batchSize,
  };

  const result = await fetchOracleDataWithPagination(
    connection,
    query,
    context,
  );
  return result.rows;
};

export const estimateTableRowCount = async (
  connection: oracledb.Connection,
  tableName: string,
): Promise<number> => {
  try {
    // Try to get estimate from Oracle statistics first (faster)
    const statsQuery = `
            SELECT NUM_ROWS 
            FROM USER_TABLES 
            WHERE TABLE_NAME = UPPER('${tableName}')
        `;

    const statsResult = await connection.execute(statsQuery);

    if (statsResult.rows && statsResult.rows.length > 0) {
      const firstRow = statsResult.rows[0] as any[];
      if (firstRow && firstRow[0] != null) {
        return Number(firstRow[0]);
      }
    }

    // Fallback to COUNT(*) - slower but accurate
    const countQuery = `SELECT COUNT(*) FROM ${tableName}`;
    const countResult = await connection.execute(countQuery);

    if (countResult.rows && countResult.rows.length > 0) {
      const firstRow = countResult.rows[0] as any[];
      return Number(firstRow?.[0] || 0);
    }

    return 0;
  } catch (error) {
    await logError(
      `Error estimating row count for ${tableName}: ${(error as Error).message}`,
    );
    return 0;
  }
};

export const checkIndexExists = async (
  connection: oracledb.Connection,
  tableName: string,
  columnName: string,
): Promise<boolean> => {
  try {
    const query = `
            SELECT COUNT(*) 
            FROM USER_IND_COLUMNS ic
            JOIN USER_INDEXES i ON ic.INDEX_NAME = i.INDEX_NAME
            WHERE ic.TABLE_NAME = UPPER('${tableName}')
            AND ic.COLUMN_NAME = UPPER('${columnName}')
        `;

    const result = await connection.execute(query);

    if (result.rows && result.rows.length > 0) {
      const firstRow = result.rows[0] as any[];
      return Number(firstRow?.[0] || 0) > 0;
    }

    return false;
  } catch (error) {
    await logError(
      `Error checking index for ${tableName}.${columnName}: ${(error as Error).message}`,
    );
    return false;
  }
};

export const closeOracleConnection = async (
  connection: oracledb.Connection,
): Promise<void> => {
  try {
    await connection.close();
    await logInfo("Oracle connection closed");
  } catch (error) {
    await logError(
      `Error closing Oracle connection: ${(error as Error).message}`,
    );
  }
};
