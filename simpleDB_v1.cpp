// ref:https://cstack.github.io/db_tutorial/
#define _CRT_SECURE_NO_WARNINGS
#include<iostream>
#include <string>
using namespace std;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100

typedef enum {
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum { 
	PREPARE_SUCCESS, 
	PREPARE_SYNTAX_ERROR,
	PREPARE_UNRECOGNIZED_STATEMENT 
} PrepareResult;

typedef enum { 
	STATEMENT_INSERT, 
	STATEMENT_SELECT 
} StatementType;

typedef enum {
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct {
	char buffer[50];
	long int input_length; //ssize_t
} InputBuffer;

typedef struct {
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE];
	char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct {
	StatementType type;
	Row row_to_insert; 
} Statement;

typedef struct {
	uint32_t num_rows;
	void* pages[TABLE_MAX_PAGES];
} Table;

const uint32_t ID_SIZE = sizeof(((Row*)0)->id);
const uint32_t USERNAME_SIZE = sizeof(((Row*)0)->username);
const uint32_t EMAIL_SIZE = sizeof(((Row*)0)->email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

InputBuffer* newInputBuffer();
void printPrompt();
void readInput(InputBuffer* inputBuffer);
void closeInputBuffer(InputBuffer* inputBuffer);
MetaCommandResult doMetaCommand(InputBuffer* inputBuffer);
PrepareResult prepareStatement(InputBuffer* inputBuffer, Statement* statement);
ExecuteResult executeInsert(Statement* statement, Table* table);
ExecuteResult executeSelect(Statement* statement, Table* table);
ExecuteResult executeStatement(Statement* statement, Table* table);
Table* newTable();
void freeTable(Table* table);
void serializeRow(Row* source, Row* destination);
void deserializeRow(Row* source, Row* destination);
void printRow(Row* row);
Row* rowSlot(Table* table, uint32_t rowNum);


InputBuffer* newInputBuffer() {
	InputBuffer* inputBuffer = new InputBuffer;
	memset(inputBuffer->buffer, '\0', 50);
	inputBuffer->input_length = 0;
	return inputBuffer;
}

void printPrompt() {
	cout << "simpleDB > ";
}

void readInput(InputBuffer* inputBuffer) {
	string input;
	getline(cin, input);
	long int bytesRead = input.size();

	if (bytesRead <= 0) {
		cout << " Error reading input" << endl;
		exit(1);
	}

	inputBuffer->input_length = bytesRead;
	for (int i = 0; i < bytesRead; i++) {
		inputBuffer->buffer[i] = input[i];
	}
	inputBuffer->buffer[bytesRead] = '\0';
}

void closeInputBuffer(InputBuffer* inputBuffer) {
	delete inputBuffer->buffer;
	delete inputBuffer;
}

MetaCommandResult doMetaCommand(InputBuffer* inputBuffer) {
	if (strcmp(inputBuffer->buffer, ".exit") == 0) {
		exit(0);
	}
	else {
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

PrepareResult prepareStatement(InputBuffer* inputBuffer, Statement* statement) {
	if (strncmp(inputBuffer->buffer, "insert", 6) == 0) {
		statement->type = STATEMENT_INSERT;
		int argsAssigned = sscanf(inputBuffer->buffer, "insert %d %s %s", 
			&statement->row_to_insert.id,
			&statement->row_to_insert.username, 
			&statement->row_to_insert.email);
		if (argsAssigned < 3) {
			return PREPARE_SYNTAX_ERROR;
		}
		return PREPARE_SUCCESS;
	}
	else if (strcmp(inputBuffer->buffer, "select") == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	else {
		return PREPARE_UNRECOGNIZED_STATEMENT;
	}
}

ExecuteResult executeInsert(Statement* statement, Table* table) {
	if (table->num_rows >= TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL;
	}
	Row* rowToInsert = &statement->row_to_insert;
	serializeRow(rowToInsert, (Row*)rowSlot(table, table->num_rows));
	table->num_rows = table->num_rows + 1;

	return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement* statement, Table* table) {
	Row row;
	for (uint32_t i = 0; i < table->num_rows; i++) {
		deserializeRow((Row*)rowSlot(table, i), &row);
		printRow(&row);
	}

	return EXECUTE_SUCCESS;
}

ExecuteResult executeStatement(Statement* statement, Table* table) {
	switch (statement->type) {
	case STATEMENT_INSERT:
		return executeInsert(statement, table);
	case STATEMENT_SELECT:
		return executeSelect(statement, table);
	}
}

Table* newTable() {
	Table* table = new Table;
	table->num_rows = 0;
	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		table->pages[i] = NULL;
	}
	return table;
}

void freeTable(Table* table) {
	for (int i = 0; table->pages[i]; i++) {
		delete table->pages[i];
	}
	delete table;
}

void serializeRow(Row* source, Row* destination) {
	/*cout << destination << endl;
	cout << ID_OFFSET << " " << USERNAME_OFFSET << " " << EMAIL_OFFSET << endl;
	cout << source->id << " ";
	cout << source->username << " ";
	cout << source->email << endl;*/

	memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
	memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
	memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserializeRow(Row* source, Row* destination) {
	memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
	memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void printRow(Row* row) {
	cout << "(" << row->id << ",";
	cout << row->username << ",";
	cout << row->email << ")" << endl;
}

Row* rowSlot(Table* table, uint32_t rowNum) {
	uint32_t pageNum = rowNum / ROWS_PER_PAGE;
	void* page = table->pages[pageNum];
	if (page == NULL) {
		table->pages[pageNum] = malloc(PAGE_SIZE);
		page = table->pages[pageNum];
	}
	uint32_t rowOffset = rowNum % ROWS_PER_PAGE;
	uint32_t byteOffset = rowOffset * ROW_SIZE;
	return (Row*)((char*)page + byteOffset);
}


int main() {
	Table* table = newTable();
	InputBuffer* inputBuffer = newInputBuffer();
	while (1) {
		printPrompt();
		readInput(inputBuffer);
		if (inputBuffer->buffer[0] == '.') {
			switch (doMetaCommand(inputBuffer)) {
			case META_COMMAND_SUCCESS:
				continue;
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				cout << "Unrecognized command '";
				cout << inputBuffer->buffer;
				cout << "'." << endl;
				continue;
			}
		}

		Statement statement;
		switch (prepareStatement(inputBuffer, &statement)) {
		case PREPARE_SUCCESS:
			break;
		case PREPARE_SYNTAX_ERROR:
			cout << "Syntax Error.Could not parse statement." << endl;
			continue;
		case PREPARE_UNRECOGNIZED_STATEMENT:
			cout << "Unrecognized keyword at start of '";
			cout << inputBuffer->buffer;
			cout << "'." << endl;
			continue;
		}

		switch (executeStatement(&statement, table)) {
		case EXECUTE_SUCCESS:
			cout << "Executed." << endl;
			break;
		case EXECUTE_TABLE_FULL:
			cout << "Error:Table Full." << endl;
			break;
		}
		//closeInputBuffer(inputBuffer);
	}
}