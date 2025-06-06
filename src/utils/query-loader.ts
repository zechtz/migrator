import fs, { readFileSync } from "fs";
import { resolve } from "path";

/**
 * Load and process SQL query from file
 */
export function loadQuery(
  filename: string,
  variables?: Record<string, string>,
): string {
  try {
    const queryPath = resolve(process.cwd(), "src", "queries", filename);
    let query = readFileSync(queryPath, "utf8");

    // Replace variables in the query
    if (variables) {
      for (const [key, value] of Object.entries(variables)) {
        const placeholder = `{{${key}}}`;
        query = query.replace(new RegExp(placeholder, "g"), value);
      }
    }

    // Clean up the query (remove extra whitespace)
    return query.trim().replace(/\s+/g, " ");
  } catch (error) {
    throw new Error(
      `Failed to load query from ${filename}: ${(error as Error).message}`,
    );
  }
}

/**
 * Load query with environment variable substitution
 */
export function loadQueryWithEnv(
  filename: string,
  defaultVars?: Record<string, string>,
): string {
  const variables = {
    START_DATE: process.env.MIGRATION_START_DATE || "01-JANUARY-2025",
    END_DATE: process.env.MIGRATION_END_DATE || "31-DECEMBER-2025",
    ...defaultVars,
  };

  return loadQuery(filename, variables);
}

/**
 * Get all available query files
 */
export function getAvailableQueries(): string[] {
  try {
    const queriesDir = resolve(process.cwd(), "src", "queries");
    return fs
      .readdirSync(queriesDir)
      .filter((file: string) => file.endsWith(".sql"))
      .map((file: string) => file.replace(".sql", ""));
  } catch (error) {
    return [];
  }
}
