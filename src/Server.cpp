#include <cstring>
#include <iostream>
#include <fstream>

constexpr int HEADER_SIZE = 100;

unsigned short parse_short (const char *buffer) {
    /* big endian */
    return ((static_cast<unsigned char>(buffer[0]) << 8) | static_cast<unsigned char>(buffer[1]));
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Expected two arguments" << std::endl;
        return 1;
    }

    std::string database_file_path = argv[1];
    std::string command = argv[2];

    if (command == ".dbinfo") {
        std::ifstream database_file;
        database_file.open(database_file_path, std::ios::binary);

        if (!database_file) {
            std::cerr << "Failed to open the database file" << std::endl;
            return 1;
        }

        database_file.seekg(16, std::ios::beg);  // Skip the first 16 bytes of the header
        char page_size_buffer[2];
        database_file.read(page_size_buffer, 2);
        unsigned short page_size = parse_short(page_size_buffer);

        database_file.seekg(HEADER_SIZE + 3, std::ios::beg);
        char table_count_buffer[2];
        database_file.read(table_count_buffer, 2);
        unsigned short table_count = parse_short(table_count_buffer);

        printf("database page size: %u \n", page_size);
        printf("number of tables: %u \n", table_count);
    }

    return 0;
}
