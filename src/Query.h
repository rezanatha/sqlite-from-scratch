#ifndef QUERY_H
#define QUERY_H

#include<iostream>
#include<algorithm>
#include<stack>
#include<vector>
#include<string>
#include<sstream>
#include<map>
#include <assert.h>
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
        std::string definition_of;

        //for CREATE TABLE
        std::vector<std::string> columns;
        std::vector<std::string> type;
        std::vector<std::string> additional_statement;

        //for CREATE INDEX
        std::string index_target;
        std::string index_key;
    };
    /* FUNCTIONS TO PARSE QUERY AND TABLE DEFINITION */
    std::string lower_string (std::string s);
    std::string get_where_condition_string_value (std::string &s);
    DQLStatement parse_query (const std::string &q);
    std::vector<std::string> get_column (const std::string &query, size_t &offset);
    std::string get_table_name (const std::string &query, size_t &offset);
    std::vector<WhereCondition> get_where_clause (const std::string &query, size_t &offset);
    DDLStatement parse_table_definition (
        const std::string &table_name,
        const std::string &table_definition);

    /* FUNCTIONS FOR PROCESSING PAGES & CELLS */
    void process_index_leaf (
        Database::IndexLeafPage &page,
        const uint32_t page_size,
        const Query::DQLStatement &query,
        const Query::DDLStatement &def
    );
    void process_index_interior (
        Database::IndexInteriorPage &page,
        const uint32_t page_size,
        const Query::DQLStatement &query,
        const Query::DDLStatement &def
    );
    void process_index_leaf (
        Database::IndexLeafPage &page,
        const uint32_t page_size,
        const Query::DQLStatement &query,
        const Query::DDLStatement &def
    );
    void fetch_query_result (
        const DQLStatement &query, 
        const DDLStatement &table_def, 
        std::vector<Database::TableLeafCell> &table,
        uint32_t id
    );
    std::vector<Database::TableLeafCell> do_index_scan (
        std::vector<size_t> &row_ids,
        const Query::DQLStatement &query,
        const DDLStatement &table_def,
        const uint32_t page_size, 
        Database::PageHeader &table_to_process
    );
    void process_select_from_statement(
        const DQLStatement &query, 
        const std::map<std::string, std::vector<Database::PageHeader>> &table_map, 
        uint32_t page_size,
        bool use_index
    );
}

#endif