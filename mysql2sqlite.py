#!/usr/bin/env python3

import sys
import re
import os
import glob

# Authors: @esperlu, @artemyk, @gkuenning, @dumblob

def printerr(s):
    print(s, file=sys.stderr)

def bit_to_int(str_bit):
    powtwo = 1
    overflow = 0
    res = 0
    # 011101 = 1 * 2^0 + 0 * 2^1 + 1 * 2^2 ...
    for i in range(len(str_bit), 0, -1):
        bit = int(str_bit[i - 1])
        if overflow or (bit == 1 and res > INT_MAX_HALF):
            printerr(f"{NR}: WARN Bit field overflow, number truncated (LSBs saved, MSBs ignored).")
            break
        res = res + bit * powtwo
        # no warning here as it might be the last iteration
        if powtwo > INT_MAX_HALF:
            overflow = 1
            continue
        powtwo = powtwo * 2
    return res

def convert_mysql_to_sqlite(input_file, output_file):
    global INT_MAX_HALF, NR, inTrigger, inView, tableName, aInc, prev, firstInTable, key, caseIssue, hexIssue

    inTrigger = False
    inView = False
    tableName = ""
    aInc = 0
    prev = ""
    firstInTable = True
    key = {}
    caseIssue = False
    hexIssue = False

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for NR, line in enumerate(infile, 1):
            line = line.rstrip()

            # CREATE TRIGGER statements have funny commenting. Remember we are in trigger.
            if re.match(r'^/\*.*(CREATE.*TRIGGER|create.*trigger)', line):
                line = re.sub(r'^.*(TRIGGER|trigger)', 'CREATE TRIGGER', line)
                outfile.write(line + '\n')
                inTrigger = True
                continue
            # The end of CREATE TRIGGER has a stray comment terminator
            if re.match(r'(END|end) \*/;;', line):
                line = re.sub(r'\*/', '', line)
                outfile.write(line + '\n')
                inTrigger = False
                continue
            # The rest of triggers just get passed through
            if inTrigger:
                outfile.write(line + '\n')
                continue

            # CREATE VIEW looks like a TABLE in comments
            if re.match(r'^/\*.*(CREATE.*TABLE|create.*table)', line):
                inView = True
                continue
            # end of CREATE VIEW
            if re.match(r'^(\).*(ENGINE|engine).*\*/;)', line):
                inView = False
                continue
            # content of CREATE VIEW
            if inView:
                continue

            # skip comments
            if re.match(r'^/\*', line):
                continue

            # skip PARTITION statements
            if re.match(r'^ *[(]?(PARTITION|partition) +[^ ]+', line):
                continue

            # print all INSERT lines
            if (re.match(r'^ *$', line) and re.match(r'$ *[,;] *$', line)) or re.match(r'^(INSERT|insert|REPLACE|replace)', line):
                prev = ""

                # first replace \\ by \_ that mysqldump never generates to deal with
                # sequnces like \\n that should be translated into \n, not \<LF>.
                # After we convert all escapes we replace \_ by backslashes.
                line = line.replace('\\\\', '\\_')

                # single quotes are escaped by another single quote
                line = line.replace("\\'", "''")
                line = line.replace("\\n", "\n")
                line = line.replace("\\r", "\r")
                line = line.replace('\\"', '"')
                line = line.replace("\\\032", "\032")  # substitute char

                line = line.replace("\\_", "\\")

                # sqlite3 is limited to 16 significant digits of precision
                while re.search(r'0x[0-9a-fA-F]{17}', line):
                    hexIssue = True
                    line = re.sub(r'0x[0-9a-fA-F]+', lambda m: m.group(0)[:-1], line)
                if hexIssue:
                    printerr(f"{NR}: WARN Hex number trimmed (length longer than 16 chars).")
                    hexIssue = False
                outfile.write(line + '\n')
                continue

            # CREATE DATABASE is not supported
            if re.match(r'^(CREATE DATABASE|create database)', line):
                continue

            # print the CREATE line as is and capture the table name
            if re.match(r'^(CREATE|create)', line):
                if re.search(r'IF NOT EXISTS|if not exists', line) or re.search(r'TEMPORARY|temporary', line):
                    caseIssue = True
                    printerr(
                        f"{NR}: WARN Potential case sensitivity issues with table/column naming\n"
                        "          (see INFO at the end)."
                    )
                match = re.search(r'`[^`]+', line)
                if match:
                    tableName = match.group(0)[1:-1]
                aInc = 0
                prev = ""
                firstInTable = True
                outfile.write(line + '\n')
                continue

            # Replace `FULLTEXT KEY` (probably other `XXXXX KEY`)
            if re.match(r'^  (FULLTEXT KEY|fulltext key)', line):
                line = re.sub(r'[A-Za-z ]+(KEY|key)', '  KEY', line)

            # Get rid of field lengths in KEY lines
            if re.search(r' (PRIMARY |primary )?(KEY|key)', line):
                line = re.sub(r'$[0-9]+$', '', line)

            if aInc == 1 and re.search(r'PRIMARY KEY|primary key', line):
                continue

            # Replace COLLATE xxx_xxxx_xx statements with COLLATE BINARY
            if re.search(r' (COLLATE|collate) [a-z0-9_]*', line):
                line = re.sub(r'(COLLATE|collate) [a-z0-9_]*', 'COLLATE BINARY', line)

            # Print all fields definition lines except the `KEY` lines.
            if re.match(r'^  ', line) and not re.match(r'^(  (KEY|key)|\);)', line):
                if re.search(r'[^"`]AUTO_INCREMENT|auto_increment[^"`]', line):
                    aInc = 1
                    line = re.sub(r'AUTO_INCREMENT|auto_increment', 'PRIMARY KEY AUTOINCREMENT', line)
                line = re.sub(r'(UNIQUE KEY|unique key) (`.*`|".*") ', 'UNIQUE ', line)
                line = re.sub(r'(CHARACTER SET|character set) [^ ]+[ ,]', '', line)
                # FIXME
                #   CREATE TRIGGER [UpdateLastTime]
                #   AFTER UPDATE
                #   ON Package
                #   FOR EACH ROW
                #   BEGIN
                #   UPDATE Package SET LastUpdate = CURRENT_TIMESTAMP WHERE ActionId = old.ActionId;
                #   END
                line = re.sub(r'(ON|on) (UPDATE|update) (CURRENT_TIMESTAMP|current_timestamp)($$)?', '', line)
                line = re.sub(r'(DEFAULT|default) (CURRENT_TIMESTAMP|current_timestamp)($$)?', 'DEFAULT current_timestamp', line)
                line = re.sub(r'(COLLATE|collate) [^ ]+ ', '', line)
                line = re.sub(r'(ENUM|enum)[^)]+\)', 'text ', line)
                line = re.sub(r'(SET|set)$[^)]+$', 'text ', line)
                line = re.sub(r'UNSIGNED|unsigned', '', line)
                line = re.sub(r'_utf8mb3', '', line)
                line = re.sub(r'` [^ ]*(INT|int|BIT|bit)[^ ]*', '` integer', line)
                line = re.sub(r'" [^ ]*(INT|int|BIT|bit)[^ ]*', '" integer', line)
                ere_bit_field = r"[bB]'[10]+'"
                match = re.search(ere_bit_field, line)
                if match:
                    line = re.sub(ere_bit_field, str(bit_to_int(match.group(0)[2:-1])), line)

                # remove USING BTREE and other suffixes for USING, for example: "UNIQUE KEY
                # `hostname_domain` (`hostname`,`domain`) USING BTREE,"
                line = re.sub(r' USING [^, ]+', '', line)

                # field comments are not supported
                line = re.sub(r' (COMMENT|comment).+$', '', line)
                # Get commas off end of line
                line = re.sub(r',.?$', '', line)
                if prev:
                    if firstInTable:
                        outfile.write(prev + '\n')
                        firstInTable = False
                    else:
                        outfile.write("," + prev + '\n')
                else:
                    # FIXME check if this is correct in all cases
                    if re.match(r'(CONSTRAINT|constraint) ["].*["] (FOREIGN KEY|foreign key)', line):
                        outfile.write(",\n")
                prev = line

            if re.search(r' ENGINE| engine', line):
                if prev:
                    if firstInTable:
                        outfile.write(prev + '\n')
                        firstInTable = False
                    else:
                        outfile.write("," + prev + '\n')
                prev = ""
                outfile.write(");\n")
                continue

            # `KEY` lines are extracted from the `CREATE` block and stored in array for later print
            # in a separate `CREATE KEY` command. The index name is prefixed by the table name to
            # avoid a sqlite error for duplicate index name.
            if re.match(r'^(  (KEY|key)|\);)', line):
                if prev:
                    if firstInTable:
                        outfile.write(prev + '\n')
                        firstInTable = False
                    else:
                        outfile.write("," + prev + '\n')
                prev = ""
                if line == ");":
                    outfile.write(line + '\n')
                else:
                    match = re.search(r'`[^`]+', line)
                    if match:
                        indexName = match.group(0)[1:-1]
                    match = re.search(r'\([^()]+', line)
                    if match:
                        indexKey = match.group(0)[1:-1]
                    # idx_ prefix to avoid name clashes (they really happen!)
                    key[tableName] = key.get(tableName, "") + f'CREATE INDEX "idx_{tableName}_{indexName}" ON "{tableName}" ({indexKey});\n'
                continue

        # print all KEY creation lines.
        for table in key:
            outfile.write(key[table])

        outfile.write("END TRANSACTION;\n")

        if caseIssue:
            printerr(
                "INFO Pure sqlite identifiers are case insensitive (even if quoted\n"
                "     or if ASCII) and doesnt cross-check TABLE and TEMPORARY TABLE\n"
                "     identifiers. Thus expect errors like \"table T has no column named F\"."
            )

def main():
    global INT_MAX_HALF

    # Find INT_MAX supported by both this Python (usually an ISO C signed int)
    #   and SQlite.
    # On non-8bit-based architectures, the additional bits are safely ignored.

    # 8bit (lower precision should not exist)
    s = "127"
    # "63" + 0 avoids potential parser misbehavior
    if int(s) == int(s):
        INT_MAX_HALF = 63
    # 16bit
    s = "32767"
    if int(s) == int(s):
        INT_MAX_HALF = 16383
    # 32bit
    s = "2147483647"
    if int(s) == int(s):
        INT_MAX_HALF = 1073741823
    # 64bit (as INTEGER in SQlite3)
    s = "9223372036854775807"
    if int(s) == int(s):
        INT_MAX_HALF = 4611686018427387904

    # Find all .sql files in the current directory
    sql_files = glob.glob("*.sql")

    if not sql_files:
        printerr("No .sql files found in the current directory.")
        sys.exit(1)

    for sql_file in sql_files:
        output_file = os.path.splitext(sql_file)[0] + "_sqlite.sql"
        print(f"Converting {sql_file} to {output_file}...")
        convert_mysql_to_sqlite(sql_file, output_file)
        print(f"Conversion of {sql_file} completed.")

if __name__ == "__main__":
    main()