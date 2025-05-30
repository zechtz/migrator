import oracledb from "oracledb";
import dotenv from "dotenv";

dotenv.config();

async function checkOracleTables() {
  let connection;

  try {
    const requiredEnvVars = [
      "ORACLE_USER",
      "ORACLE_PASSWORD",
      "ORACLE_HOST",
      "ORACLE_PORT",
      "ORACLE_SID",
    ];

    const missingVars = requiredEnvVars.filter(
      (varName) => !process.env[varName],
    );

    if (missingVars.length > 0) {
      throw new Error(
        `Missing required environment variables: ${missingVars.join(", ")}`,
      );
    }

    // Connect using environment variables
    connection = await oracledb.getConnection({
      user: process.env.ORACLE_USER!,
      password: process.env.ORACLE_PASSWORD!,
      connectString: `(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=${process.env.ORACLE_HOST})(PORT=${process.env.ORACLE_PORT}))(CONNECT_DATA=(SID=${process.env.ORACLE_SID})))`,
    });

    console.log("🔍 Connected to Oracle. Checking available tables...\n");

    // Check all accessible tables
    const allTablesQuery = `
      SELECT table_name, owner 
      FROM all_tables 
      WHERE table_name IN ('PERSON', 'BIRTH_REGISTRATION', 'SEX', 'PLACE_OF_BIRTH', 'REGION', 'DISTRICT', 'WARD', 'COUNTRY')
      ORDER BY owner, table_name
    `;

    const allTablesResult = await connection.execute(allTablesQuery);

    console.log("📋 Tables found in database:");
    console.log("============================");

    if (!allTablesResult.rows || allTablesResult.rows.length === 0) {
      console.log("❌ No matching tables found!");
    } else {
      allTablesResult.rows.forEach((row: any) => {
        console.log(`✅ ${row[1]}.${row[0]}`); // OWNER.TABLE_NAME
      });
    }

    console.log("\n🔍 Checking current user and schema:");
    console.log("===================================");

    // Check current user
    const userQuery = `SELECT USER FROM DUAL`;
    const userResult: any = await connection.execute(userQuery);
    if (userResult.rows && userResult.rows.length > 0) {
      console.log(`Current user: ${userResult.rows[0][0]}`);
    }

    // Check all schemas/owners that have these tables
    const ownersQuery = `
      SELECT DISTINCT owner 
      FROM all_tables 
      WHERE table_name IN ('CRVS.PERSON', 'CRVS.BIRTH_REGISTRATION', 'CRVS.SEX', 'CRVS.PLACE_OF_BIRTH')
      ORDER BY owner
    `;

    const ownersResult = await connection.execute(ownersQuery);

    console.log("\n📂 Schemas containing these tables:");
    console.log("==================================");

    if (ownersResult.rows && ownersResult.rows.length > 0) {
      ownersResult.rows.forEach((row: any) => {
        console.log(`• ${row[0]}`);
      });
    } else {
      console.log("❌ No schemas found with these tables");
    }

    // Try a simple query on each table to see which ones work
    const tables = [
      "PERSON",
      "BIRTH_REGISTRATION",
      "SEX",
      "PLACE_OF_BIRTH",
      "REGION",
      "DISTRICT",
      "WARD",
      "COUNTRY",
    ];

    console.log("\n🧪 Testing table access:");
    console.log("========================");

    for (const table of tables) {
      try {
        const testQuery = `SELECT COUNT(*) FROM ${table} WHERE ROWNUM <= 1`;
        const result = await connection.execute(testQuery);
        if (result.rows && result.rows.length > 0) {
          console.log(`✅ ${table}: Accessible (has data)`);
        }
      } catch (error: any) {
        console.log(`❌ ${table}: ${error.message.split("\n")[0]}`);

        // Try with different schema prefixes
        const commonSchemas = [
          "CRVS",
          "REGISTRY",
          "PUBLIC",
          "SYSTEM",
          "BRS4G",
          "GSMADMIN_INTERNAL",
        ];
        for (const schema of commonSchemas) {
          try {
            const schemaQuery = `SELECT COUNT(*) FROM ${schema}.${table} WHERE ROWNUM <= 1`;
            const schemaResult = await connection.execute(schemaQuery);
            if (schemaResult.rows && schemaResult.rows.length > 0) {
              console.log(`   ✅ Found as: ${schema}.${table}`);
              break;
            }
          } catch {
            // Continue to next schema
          }
        }
      }
    }

    // Also check what tables the current user can see
    console.log("\n📋 All tables visible to current user:");
    console.log("====================================");

    const userTablesQuery = `SELECT table_name FROM user_tables ORDER BY table_name`;
    const userTablesResult = await connection.execute(userTablesQuery);

    if (userTablesResult.rows && userTablesResult.rows.length > 0) {
      console.log("User owns these tables:");
      userTablesResult.rows.slice(0, 20).forEach((row: any) => {
        // Show first 20
        console.log(`• ${row[0]}`);
      });
      if (userTablesResult.rows.length > 20) {
        console.log(`... and ${userTablesResult.rows.length - 20} more`);
      }
    } else {
      console.log("❌ Current user owns no tables");
    }
  } catch (error: any) {
    console.error("❌ Error:", error.message);

    // Provide helpful error messages for common issues
    if (error.message.includes("Missing required environment variables")) {
      console.error(
        "\n💡 Make sure to create a .env file with the following variables:",
      );
      console.error("ORACLE_USER=your_username");
      console.error("ORACLE_PASSWORD=your_password");
      console.error("ORACLE_HOST=your_host");
      console.error("ORACLE_PORT=your_port");
      console.error("ORACLE_SID=your_sid");
    }
  } finally {
    if (connection) {
      await connection.close();
    }
  }
}

checkOracleTables();
