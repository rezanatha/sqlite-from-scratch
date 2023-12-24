#include <cstring>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <memory>
#include <sstream>
#include <algorithm>
#include "Decode.h"
#include "Database.h"
#include "Query.h"

std::vector<std::string> split_string (const std::string &str, const char delimiter) {
    std::vector<std::string> output;
    std::stringstream ss(str);
    std::string token;

    while (std::getline(ss, token, delimiter) ) {
        output.push_back(token);
    }
    return output;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    std::string database_file_path = argv[1];
    std::string command = argv[2];
    
    std::shared_ptr<std::ifstream> db_object (new std::ifstream(database_file_path, std::ios::binary));
    Database::db = db_object.get();


    if (!Database::db) {
        std::cerr << "Failed to open the database file" << std::endl;
        return 1;
    }
    
    uint32_t page_size = Database::db_header_page_size();
    uint32_t num_page = Database::db_header_num_of_pages();
    uint32_t encoding = Database::db_header_encoding();

    uint16_t cell_count = Database::master_header_cell_count();

    // Process master table
    std::vector<Database::Cell> master_table = Database::read_master_table();
    std::map<std::string, Database::Cell*> table_map;

    for (auto &row: master_table) {
        std::string table_name = *static_cast<std::string*>(row.field[2].field_value);
        //uint16_t root_page_num = *static_cast<uint16_t*>(row.field[3].field_value);
        table_map[table_name] = &row;
    }  

    //Process command
    if (command == ".dbinfo") {
        printf("database page size: %u \n", page_size);
        //printf("number of pages: %u \n", num_page);
        printf("number of tables: %u \n", cell_count); 
        //printf("encoding: %u \n", encoding); 

    } else if (command == ".tables") {
        for (auto r: master_table) {
            //2 = table name 4 = table definition
            //printf("row size %zu, row_id %d ", r.row_size, r.row_id);
            uint32_t field_type = r.field[4].field_type;
            int field_size = r.field[4].field_size;
            std::string s = *static_cast<std::string*>(r.field[2].field_value);
            printf("%s \n", s.c_str());
        }

    } else {
        Query::DQLStatement query = Query::parse_query(command);
        Query::process_select_from_statement(query, table_map, page_size);

    }
    return 0;
}
