#ifndef TABLE_H
#define TABLE_H

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define TABLE_MAX_PAGES 100
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

#define INVALID_PAGE_NUM UINT32_MAX

typedef struct {
	int file_descriptor;
	uint32_t file_length;
	uint32_t num_pages;
	void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE + 1];
	char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
	Pager* pager;
	uint32_t root_page_num;
} Table;

typedef struct {
    Table* table;
    uint32_t page_num;
	uint32_t cell_num;
    bool end_of_table;
} Cursor;

static const uint32_t ID_SIZE = size_of_attribute(Row, id);
static const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
static const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
static const uint32_t ID_OFFSET = 0;
static const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
static const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
static const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
static const uint32_t PAGE_SIZE = 4096;
static const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
static const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

void serialize_row(Row* source, void* destination);

void deserialize_row(void* source, Row* destination);

uint32_t get_unused_page_num(Pager* pager);

Pager* pager_open(const char* filename);

void pager_flush(Pager* pager, uint32_t page_num);

void* get_page(Pager* pager, uint32_t page_num);

Table* db_open(const char* filename);

void db_close(Table* table);

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key);

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key);

Cursor* table_find(Table* table, uint32_t key);

Cursor* table_start(Table* table);

void* cursor_value(Cursor* cursor);

void cursor_advance(Cursor* cursor);

#endif // TABLE_H