package main

import (
    "fmt"
    "bufio"
    "os"
    "strings"
    "unsafe"
)

const ROW_SIZE  = int(unsafe.Sizeof(Row{}))
const PAGE_SIZE int = 4096
const TABLE_MAX_PAGES int = 100
const ROWS_PER_PAGE int = PAGE_SIZE / ROW_SIZE
const TABLE_MAX_ROWS int = TABLE_MAX_PAGES * ROWS_PER_PAGE

type MetaCmdResult int
type PrepareResult int
type ExecuteResult int
type StatementType int
type Statement struct {
    stype           StatementType
    row_to_insert   Row
}
type Row struct {
    id      int
    name    [32]byte
    email   [255]byte
}
type Table struct {
    row_num     int
    pages       [TABLE_MAX_PAGES]Page
    page_bool   [TABLE_MAX_PAGES]bool
}
type Page struct {
    rows        [ROWS_PER_PAGE]Row
}


const(
    META_COMMAND_SUCCESS            MetaCmdResult = iota
    META_COMMAND_UNRECOGNIZED
)

const(
    PREPARE_SUCCESS                 PrepareResult = iota
    PREPARE_UNRECOGNIZED
    PREPARE_SYNTAX_ERROR
)

const(
    EXECUTE_SUCCESS                 ExecuteResult = iota
    EXECUTE_TABLE_FULL
)

const(
    STATEMENT_SELECT                StatementType = iota
    STATEMENT_INSERT
)


var inputReader *bufio.Reader

func main(){
    inputReader = bufio.NewReader(os.Stdin)
    var table *Table = new_table()
    defer free_table(table)
    for {
        print_prompt()
        input := read_input()
        exec_cmd(input, table)
    }
}

func print_prompt() {
    fmt.Print("db >")
}

func read_input() string{
    input, err := inputReader.ReadString('\n')
    if err != nil {
        fmt.Println("Read input error")
        return ""
    }
    input = strings.Trim(input, "\n")
    input = strings.Trim(input, " ")
    return input
}

func exec_cmd(input string, table *Table) {
    if(len(input)==0){
        return
    }
    if(input[0]=='.'){
        switch do_meta_command(input, table){
        case META_COMMAND_SUCCESS:
        case META_COMMAND_UNRECOGNIZED:
            fmt.Printf("Unrecognized meta cmd '%s'\n", input)
            return
        }
    }
    var statement Statement
    switch prepare_statement(input, &statement) {
        case PREPARE_SUCCESS:
        case PREPARE_UNRECOGNIZED:
            fmt.Printf("Unrecognized keyword '%s'\n", input)
            return
        case PREPARE_SYNTAX_ERROR:
            fmt.Println("Syntax error")
            return
    }
    execute_statement(&statement, table)
}

func do_meta_command(input string, table *Table) MetaCmdResult{
    if(input==".exit"){
        os.Exit(0)
    }else{
        return META_COMMAND_UNRECOGNIZED
    }
    return META_COMMAND_UNRECOGNIZED
}

func prepare_statement(input string, statement *Statement) PrepareResult{
    if(strings.HasPrefix(input, "select")){
        statement.stype = STATEMENT_SELECT
        return PREPARE_SUCCESS
    }else if(strings.HasPrefix(input, "insert")){
        var id int
        var name string
        var email string
        args_num, err := fmt.Sscanf(input, "insert %d %s %s", &id, &name, &email)
        if (args_num<3 || err !=nil){
            fmt.Printf("args_num is '%d', err is '%s'", args_num, err)
            return PREPARE_SYNTAX_ERROR
        }
        value_to_row(id, name, email, statement)
        //fmt.Printf("sizeof a row is '%d' bytes\n", ROW_SIZE)
        //fmt.Printf("row: %d, %s, %s\n", statement.row_to_insert.id, string(statement.row_to_insert.name[:]), string(statement.row_to_insert.email[:]))
        statement.stype = STATEMENT_INSERT
        return PREPARE_SUCCESS
    }else{
        return PREPARE_UNRECOGNIZED
    }
}

func execute_statement(statement *Statement, table *Table) ExecuteResult{
    var result ExecuteResult
    switch statement.stype {
        case STATEMENT_SELECT:
            result = execute_select(statement, table)
        case STATEMENT_INSERT:
            result = execute_insert(statement, table)
            fmt.Println("do insert")
    }
    return result
}

func value_to_row(id int, name string, email string, statement *Statement){
    statement.row_to_insert.id = id
    copy(statement.row_to_insert.name[:], []byte(name))
    copy(statement.row_to_insert.email[:], []byte(email))
}

func new_table() *Table {
    t := new(Table)
    return t
}

func free_table(table *Table) {
}

func serialize_row(row *Row, table *Table, page_num int, offset_in_page int){
    new_row := Row{}
    new_row.id = row.id
    new_row.name = row.name
    new_row.email = row.email
    //fmt.Printf("row to be inserted: id:%d, name:%s, email:%s\n", row.id, string(row.name[:]), string(row.email[:]))
    table.pages[page_num].rows[offset_in_page] = new_row
}

func deserialize_row(table *Table){
    for row_cursor:=0; row_cursor<table.row_num; row_cursor++ {
        var page_num int = row_cursor / ROWS_PER_PAGE
        var offset_in_page int = row_cursor % ROWS_PER_PAGE
        pages := table.pages[page_num]
        rows := pages.rows
        row := rows[offset_in_page]
        //fmt.Printf("deserialize row_slot: %d, %d\n", page_num, offset_in_page)
        fmt.Printf("id: %d, name: %s, email: %s\n", row.id, string(row.name[:]), string(row.email[:]))
    }
}

func row_slot(table *Table) (page_num int, offset_in_page int) {
    page_num = table.row_num / ROWS_PER_PAGE
    offset_in_page = table.row_num % ROWS_PER_PAGE
    if(table.page_bool[page_num] == false){
        table.pages[page_num] = Page{}
        table.page_bool[page_num] = true
    }
    return page_num, offset_in_page
}

func execute_insert(statement *Statement, table *Table) ExecuteResult{
    if(table.row_num >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL
    }
    var row *Row = &(statement.row_to_insert)
    page_num, offset_in_page := row_slot(table)
    //fmt.Printf("row_slot: %d, %d\n", page_num, offset_in_page)
    serialize_row(row, table, page_num, offset_in_page)
    table.row_num += 1
    return EXECUTE_SUCCESS
}

func execute_select(statement *Statement, table *Table) ExecuteResult{
    deserialize_row(table)
    return EXECUTE_SUCCESS
}
