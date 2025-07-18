#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "table.h"
#include "node.h"

typedef enum {
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
	PREPARE_SUCCESS,
	PREPARE_NEGATIVE_ID,
	PREPARE_UNRECOGNIZED_COMMAND,
	PREPARE_STRING_TOO_LONG,
	PREPARE_SYNTAX_ERROR
} PrepareResult;

typedef enum {
	STATEMENT_INSERT,
	STATEMENT_SELECT
} StatementType;

typedef enum {
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL,
	EXECUTE_DUPLICATE_KEY
} ExecuteResult;

typedef struct {
	char* buffer;
	size_t buffer_length;
	size_t input_length;
} InputBuffer;

typedef struct {
	StatementType type;
	Row row_to_insert;
} Statement;

InputBuffer* new_input_buffer() {
	InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
	input_buffer->buffer = NULL;
	input_buffer->buffer_length = 0;
	input_buffer->input_length = 0;

	return input_buffer;
}

void close_input_buffer(InputBuffer* input_buffer) {
	free(input_buffer->buffer);
	free(input_buffer);
}

void indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf("  ");
  }
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
  void* node = get_page(pager, page_num);
  uint32_t num_keys, child;

  switch (get_node_type(node)) {
    case (NODE_LEAF):
      num_keys = *leaf_node_num_cells(node);
      indent(indentation_level);
      printf("- leaf (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        indent(indentation_level + 1);
        printf("- %d\n", *leaf_node_key(node, i));
      }
      break;
    case (NODE_INTERNAL):
      num_keys = *internal_node_num_keys(node);
      indent(indentation_level);
      printf("- internal (size %d)\n", num_keys);
      if (num_keys > 0) {
		for (uint32_t i = 0; i < num_keys; i++) {
			child = *internal_node_child(node, i);
			print_tree(pager, child, indentation_level + 1);

			indent(indentation_level + 1);
			printf("- key %d\n", *internal_node_key(node, i));
		}
		child = *internal_node_right_child(node);
		print_tree(pager, child, indentation_level + 1);
	  }
      break;
  }
}

void print_prompt() {
	printf("db > ");
}

void read_input(InputBuffer* input_buffer) {
	size_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

	if (bytes_read <= 0) {
		printf("Error Reading Input\n");
		exit(EXIT_FAILURE);
	}

	input_buffer->input_length = bytes_read - 1;
	input_buffer->buffer[bytes_read - 1] = 0;
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table){
	if (strcmp(input_buffer->buffer, ".exit") == 0) {
		db_close(table);
		exit(EXIT_SUCCESS);
	} else if (strcmp(input_buffer->buffer, ".btree") == 0) {
		print_tree(table->pager, 0, 0);
		return META_COMMAND_SUCCESS;
	} else {
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
	statement->type = STATEMENT_INSERT;
	
	char* keyword = strtok(input_buffer->buffer, " ");
	char* id_string = strtok(NULL, " ");
	char* username = strtok(NULL, " ");
	char* email = strtok(NULL, " ");

	if (id_string == NULL || username == NULL || email == NULL) {
		return PREPARE_SYNTAX_ERROR;
	}

	int id = atoi(id_string);
	if (id < 0) {
		return PREPARE_NEGATIVE_ID;
	}
	if (strlen(username) > COLUMN_USERNAME_SIZE) {
		return PREPARE_STRING_TOO_LONG;
	}
	if (strlen(email) > COLUMN_EMAIL_SIZE) {
		return PREPARE_STRING_TOO_LONG;
	}

	statement->row_to_insert.id = id;
	strcpy(statement->row_to_insert.username, username);
	strcpy(statement->row_to_insert.email, email);

	return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
	if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
		return prepare_insert(input_buffer, statement);
	}
	if (strcmp(input_buffer->buffer, "select") == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}

	return PREPARE_UNRECOGNIZED_COMMAND;
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
	void* node = get_page(table->pager, table->root_page_num);
	uint32_t num_cells = (*leaf_node_num_cells(node));

	Row* row_to_insert = &(statement->row_to_insert);
    
	uint32_t key_to_insert = row_to_insert->id;
	Cursor* cursor = table_find(table, key_to_insert);

	if (cursor->cell_num < num_cells) {
		uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
		if (key_at_index == key_to_insert) {
			return EXECUTE_DUPLICATE_KEY;
		}
	}

	leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

    free(cursor);

	return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Cursor* cursor = table_start(table);

	Row row;
	while(!(cursor->end_of_table)) {
		deserialize_row(cursor_value(cursor), &row);
		printf("(%d, %s, %s)\n", row.id, row.username, row.email);
        cursor_advance(cursor);
	}

    free(cursor);

	return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
	switch (statement->type) {
		case (STATEMENT_INSERT):
			return execute_insert(statement, table);
		case (STATEMENT_SELECT):
			return execute_select(statement, table);
	}
}

int main(int argc, char* argv[]){

	if (argc < 2) {
		printf("Must supply a database filename.\n");
		exit(EXIT_FAILURE);
	}

	char* filename = argv[1];
	Table* table = db_open(filename);

	InputBuffer* input_buffer = new_input_buffer();
	while (true) {
		print_prompt();
		read_input(input_buffer);

		if (input_buffer->buffer[0] == '.') {
			switch (do_meta_command(input_buffer, table)) {
				case (META_COMMAND_SUCCESS):
					continue;
				case (META_COMMAND_UNRECOGNIZED_COMMAND):
					printf("Unrecognized command '%s'.\n", input_buffer->buffer);
					continue;
			}
		}

		Statement statement;
		switch (prepare_statement(input_buffer, &statement)) {
			case (PREPARE_SUCCESS):
				break;
			case (PREPARE_NEGATIVE_ID):
				printf("ID must be positive.\n");
				continue;
			case (PREPARE_STRING_TOO_LONG):
				printf("String is too long.\n");
				continue;
			case (PREPARE_SYNTAX_ERROR):
				printf("Syntax error. Could not parse statement.\n");
				continue;
			case (PREPARE_UNRECOGNIZED_COMMAND):
				printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
				continue;
		}

		switch (execute_statement(&statement, table)) {
			case (EXECUTE_SUCCESS):
				printf("Executed.\n");
				break;
			case (EXECUTE_TABLE_FULL):
				printf("Error: Table full.\n");
				break;
			case (EXECUTE_DUPLICATE_KEY):
				printf("Error: Duplicate key.\n");
				break;
		}
	}
	close_input_buffer(input_buffer);
	db_close(table);
	return EXIT_SUCCESS;
}