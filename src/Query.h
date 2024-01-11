#ifndef QUERY_H
#define QUERY_H

#include<iostream>
#include<algorithm>
#include<stack>
#include<vector>
#include<string>
#include<sstream>
#include<map>
#include "Query.h"
#include "Database.h"

namespace Query {
    struct WhereCondition {
        std::string column; //column
        std::string expression; //expression
        std::string value; //value
    };
    struct DQLStatement {
        std::vector<std::string> columns;
        std::string table;
        std::vector<WhereCondition> condition;

    };

    struct DDLStatement {
        std::string table;
        std::vector<std::string> columns;
        std::vector<std::string> type;
        std::vector<std::string> additional_statement;
    };

    std::vector<std::string> get_column (const std::string &query, size_t &offset);
    std::string get_table_name (const std::string &query, size_t &offset);
    std::vector<WhereCondition> get_where_clause (const std::string &query, size_t &offset);
    DQLStatement parse_query (const std::string &q);
    void process_select_from_statement(
        const DQLStatement &query, 
        const std::map<std::string, Database::TableLeafCell*> &table_map, 
        uint32_t page_size);
    DDLStatement parse_table_definition (
        const std::string &table_name,
        const std::string &table_definition);

}

#endif