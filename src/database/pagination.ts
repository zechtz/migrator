import { PaginationContext } from "../../src/types";

export const buildPaginatedQuery = (
  baseQuery: string,
  context: PaginationContext,
): string => {
  const {
    strategy,
    offset,
    batchSize,
    cursorColumn,
    lastCursorValue,
    orderByClause,
  } = context;

  switch (strategy) {
    case "rownum":
      return buildRownumQuery(baseQuery, offset, batchSize);

    case "offset":
      return buildOffsetQuery(baseQuery, offset, batchSize, orderByClause);

    case "cursor":
      return buildCursorQuery(
        baseQuery,
        batchSize,
        cursorColumn!,
        lastCursorValue,
        orderByClause,
      );

    default:
      throw new Error(`Unsupported pagination strategy: ${strategy}`);
  }
};

// Oracle ROWNUM-based pagination (default for Oracle)
const buildRownumQuery = (
  baseQuery: string,
  offset: number,
  batchSize: number,
): string => {
  return `
        SELECT * FROM (
            SELECT a.*, ROWNUM rnum FROM (
                ${baseQuery}
            ) a WHERE ROWNUM <= ${offset + batchSize}
        ) WHERE rnum > ${offset}
    `;
};

// Standard OFFSET/LIMIT pagination (better for PostgreSQL-style queries)
const buildOffsetQuery = (
  baseQuery: string,
  offset: number,
  batchSize: number,
  orderByClause?: string,
): string => {
  const queryWithOrder = orderByClause
    ? `${baseQuery} ${orderByClause}`
    : baseQuery;

  return `
        ${queryWithOrder}
        OFFSET ${offset} ROWS
        FETCH NEXT ${batchSize} ROWS ONLY
    `;
};

// Cursor-based pagination (most efficient for large datasets)
const buildCursorQuery = (
  baseQuery: string,
  batchSize: number,
  cursorColumn: string,
  lastCursorValue: any,
  orderByClause?: string,
): string => {
  let whereClause = "";

  if (lastCursorValue !== undefined && lastCursorValue !== null) {
    // Handle different data types for cursor
    const formattedValue =
      typeof lastCursorValue === "string"
        ? `'${lastCursorValue}'`
        : lastCursorValue;
    whereClause = ` AND ${cursorColumn} > ${formattedValue}`;
  }

  const defaultOrder = `ORDER BY ${cursorColumn} ASC`;
  const finalOrderBy = orderByClause || defaultOrder;

  // Add WHERE clause to base query
  const queryWithCursor = baseQuery.includes("WHERE")
    ? `${baseQuery}${whereClause}`
    : `${baseQuery} WHERE 1=1${whereClause}`;

  return `
        ${queryWithCursor}
        ${finalOrderBy}
        FETCH FIRST ${batchSize} ROWS ONLY
    `;
};

export const extractCursorValue = (rows: any[], cursorColumn: string): any => {
  if (rows.length === 0) {
    return null;
  }

  const lastRow = rows[rows.length - 1];
  return (
    lastRow[cursorColumn] ||
    lastRow[cursorColumn.toLowerCase()] ||
    lastRow[cursorColumn.toUpperCase()]
  );
};

export const determineBestPaginationStrategy = (
  totalEstimatedRows: number,
  hasIndexOnOrderColumn: boolean = false,
): "rownum" | "offset" | "cursor" => {
  // For very large tables, cursor-based is most efficient
  if (totalEstimatedRows > 1000000 && hasIndexOnOrderColumn) {
    return "cursor";
  }

  // For medium tables, OFFSET is fine
  if (totalEstimatedRows > 100000) {
    return "offset";
  }

  // For smaller tables, ROWNUM is simplest
  return "rownum";
};

export const validatePaginationConfig = (
  strategy: string,
  cursorColumn?: string,
  orderByClause?: string,
): void => {
  if (strategy === "cursor" && !cursorColumn) {
    throw new Error("Cursor pagination requires cursorColumn to be specified");
  }

  if (strategy === "offset" && !orderByClause) {
    console.warn(
      "OFFSET pagination without ORDER BY may produce inconsistent results",
    );
  }
};
