#ifndef QUERY_H
#define QUERY_H

#include<iostream>
#include<vector>
#include<string>
#include<sstream>
#include<map>
#include "Query.h"
#include "Database.h"

namespace Query {
    struct Filter {
        //column
        //expression
        //value
    };
    struct DQLStatement {
        std::vector<std::string> columns;
        std::string table;
        Filter filter;

    };

    std::vector<std::string> get_column (const std::string &query, size_t &offset);
    std::string get_table_name (const std::string &query, size_t &offset);
    DQLStatement parse_query (const std::string &q);
    void process_select_from_statement(
        const DQLStatement &query, 
        const std::map<std::string, Database::Row*> &table_map, 
        uint32_t page_size);

}

#endif