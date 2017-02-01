/**
 Copyright IBM Corporation 2016

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import SwiftKuery
#if os(Linux)
    import CSQLiteLinux
#else
    import CSQLiteDarwin
#endif

import Foundation

public enum DataType {
    case String
    case Float
    case Integer
    case Blob
    case Null
    case Double
}

public enum ActionType {
    case CreateTable
    case CreateIndex
    case AddColumn
}

public class Value {
    
    var floatValue: Float?
    var intValue: Int?
    var stringValue: String?
    var doubleValue: Double?
    var blobValue: Data?
    var type: DataType
    
    init(value: Float) {
        type = .Float
        floatValue = value
    }
    
    init(value: Double) {
        type = .Double
        doubleValue = value
    }
    
    init(value: Data) {
        type = .Blob
        blobValue = value
    }
    
    init(value: Int) {
        type = .Integer
        intValue = value
    }
    
    init(value: String) {
        type = .String
        stringValue = value
    }
    
    init(value: NSNull) {
        type = .Null
    }
    
}

public class Action {
    
    var builtStatement: String
    var actionType: ActionType
    
    init(createTable: String, keyColumnName: String, keyColumnType: DataType) {
        actionType = .CreateTable
        switch keyColumnType {
        case .Integer:
            builtStatement = "CREATE TABLE IF NOT EXISTS \(createTable) (\(keyColumnName) INTEGER PRIMARY KEY AUTOINCREMENT); "
        case .String:
            builtStatement = "CREATE TABLE IF NOT EXISTS \(createTable) (\(keyColumnName) TEXT PRIMARY KEY); "
        default:
            builtStatement = "CREATE TABLE IF NOT EXISTS \(createTable) (\(keyColumnName) INTEGER PRIMARY KEY AUTOINCREMENT); "
        }
        
    }
    
    init(createIndexOnTable: String, keyColumnName: String, ascending: Bool) {
        actionType = .CreateIndex
        
        if ascending {
            builtStatement = "CREATE INDEX IF NOT EXISTS idx_\(createIndexOnTable)_\(keyColumnName) ON \(createIndexOnTable) (\(keyColumnName) ASC);"
        } else {
            builtStatement = "CREATE INDEX IF NOT EXISTS idx_\(createIndexOnTable)_\(keyColumnName) ON \(createIndexOnTable) (\(keyColumnName) DESC);"
        }
        
    }
    
    init(addColumn: String, type: DataType, table: String) {
        
        self.actionType = .AddColumn
        
        switch type {
        case .String:
            builtStatement = "ALTER TABLE \(table) ADD COLUMN \(addColumn) TEXT;"
        case .Float:
            builtStatement = "ALTER TABLE \(table) ADD COLUMN \(addColumn) NUMERIC;"
        case .Double:
            builtStatement = "ALTER TABLE \(table) ADD COLUMN \(addColumn) REAL;"
        case .Integer:
            builtStatement = "ALTER TABLE \(table) ADD COLUMN \(addColumn) INTEGER;"
        case .Blob:
            builtStatement = "ALTER TABLE \(table) ADD COLUMN \(addColumn) BLOB;"
        default:
            builtStatement = "ALTER TABLE \(table) ADD COLUMN \(addColumn) BLOB;"
        }
        
    }
    
}

extension Data {
    func hexEncodedString() -> String {
        return map { String(format: "%02hhx", $0) }.joined()
    }
}

/// An implementation of `SwiftKuery.Connection` protocol for SQLite.
/// Please see [SQLite manual](https://sqlite.org/capi3ref.html) for details.
public class SQLiteConnection: Connection {

    /// Stores all the results of the query
    private struct Result {
        var columnNames: [String] = []
        var results: [[Any]] = [[Any]]()
        var returnedResult: Bool = false
    }

    private var connection: OpaquePointer?
    private var location: Location

    /// The `QueryBuilder` with SQLite specific substitutions.
    public var queryBuilder: QueryBuilder

    /// Initialiser to create a SwiftKuerySQLite instance
    ///
    /// - parameter location: Describes where the db is stored
    /// - parameter options:  not used currently
    ///
    /// - returns: self
    public init(_ location: Location = .inMemory, options: [ConnectionOptions]? = nil) {
        self.location = location
        self.queryBuilder = QueryBuilder()
        queryBuilder.updateSubstitutions(
            [
             QueryBuilder.QuerySubstitutionNames.ucase : "UPPER",
             QueryBuilder.QuerySubstitutionNames.lcase : "LOWER",
             QueryBuilder.QuerySubstitutionNames.len : "LENGTH"])
    }

    /// Initialiser with a path to where the DB is stored
    ///
    /// - parameter filename: The path where the DB is stored
    /// - parameter options:  not used currently
    ///
    /// - returns: self
    public convenience init(filename: String, options: [ConnectionOptions]? = nil) {
        self.init(.uri(filename), options: options)
    }

    /// Connects to the DB
    ///
    /// - parameter onCompletion: callback returning an error or a nil if successful
    public func connect(onCompletion: (QueryError?) -> ()) {
        let resultCode = sqlite3_open(location.description, &connection)
        var queryError: QueryError? = nil
        if resultCode != SQLITE_OK {
            let error: String? = String(validatingUTF8: sqlite3_errmsg(connection))
            queryError = QueryError.connection(error!)
        }
        onCompletion(queryError)
    }

    public func descriptionOf(query: Query) throws -> String {
        return try query.build(queryBuilder: queryBuilder)
    }

    /// Close the connection to the DB
    public func closeConnection() {
        if let connection = connection {
            sqlite3_close(connection)
            self.connection = nil
        }
    }

    /// Executes a query.
    ///
    /// - parameter query:        The query to execute
    /// - parameter onCompletion: The result
    public func execute(query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        do {
            let sqliteQuery = try query.build(queryBuilder: queryBuilder)
            executeQuery(query: sqliteQuery, onCompletion: onCompletion)
        }
        catch QueryError.syntaxError(let error) {
            onCompletion(.error(QueryError.syntaxError(error)))
        }
        catch {
            onCompletion(.error(QueryError.syntaxError("Failed to build the query")))
        }
    }

    /// Executes a raw query.
    ///
    /// - parameter raw:          The full raw query to execute
    /// - parameter onCompletion: The result
    public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeQuery(query: raw, onCompletion: onCompletion)
    }
    
    /// Executes an array of built actions as raw queries.
    ///
    /// - parameter raw:          The full raw query to execute
    /// - parameter onCompletion: The result
    internal func execute(_ actions: [Action], onCompletion: @escaping (() -> ())) {
        for stmt in actions {
            execute(stmt.builtStatement, onCompletion: { (QueryResult) in })
        }
        onCompletion()
    }
    
    /// Executes a statement .
    ///
    /// - parameter raw:          The full raw query to execute
    /// - parameter onCompletion: The result
    internal func execute(_ actions: [Action]) {
        for stmt in actions {
            execute(stmt.builtStatement, onCompletion: { (QueryResult) in })
        }
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called once the execution of the query is completed.
    public func execute(query: Query, parameters: [Any], onCompletion: (@escaping (QueryResult) -> ())) {
        execute(query: query, onCompletion: onCompletion)
    }

    /// Executes a raw query with parameters.
    ///
    /// - parameter raw:          The full raw query to execute
    /// - parameter onCompletion: The result
    public func execute(_ raw: String, parameters: [Any], onCompletion: @escaping ((QueryResult) -> ())) {
        executeQuery(query: raw, onCompletion: onCompletion)
    }
    
    /// Executes a raw query with array of Values to be substituted.
    ///
    /// - parameter raw:          The full raw query to execute
    /// - parameter parameters:   The values to bind into the statement
    /// - parameter onCompletion: The result
    public func execute(_ raw: String, parameters: [Value], onCompletion: @escaping ((QueryResult) -> ())) {
        
        var stmt: String! = raw
        
        // no way to bind params at the moment, so using this hacky as hell workaround until binding & statements are added
        for v in parameters {
            
            var strValue: String
            switch v.type {
            case .Null:
                strValue = "NULL"
            case .Double:
                strValue = "\(v.doubleValue)"
            case .Float:
                strValue = "\(v.floatValue)"
            case .Integer:
                strValue = "\(v.intValue)"
            case .Blob:
                strValue = "x'\(v.blobValue?.hexEncodedString())'"
            case .String:
                let temp = v.stringValue?.data(using: .utf8)?.hexEncodedString()
                strValue = "cast(x'\(temp)' as varchar)"
            }
            
            stmt.replaceSubrange(stmt.range(of: "?")!, with: strValue)
            
        }
        
        execute(stmt, onCompletion: onCompletion)
        
    }

    /// Actually executes the query
    ///
    /// - parameter query:        The query
    /// - parameter onCompletion: The result
    private func executeQuery(query: String, onCompletion: @escaping ((QueryResult) -> ())) {

        var errmsg: UnsafeMutablePointer<Int8>?
        var result = Result()

        // This is where we bridge to the C code
        // - connection:     the OpaquePointer to the DB
        // - query:          the query to execute
        // - callback:       if there are any results to be returned this will run otherwise it will skip
        //                   calling the callback
        // - result:         stores the result of the callback, if there are any
        // - errmsg:         the error message if something goes wrong
        let resultCode = sqlite3_exec(connection, query, {
            (result, cols, colText, colName) -> Int32 in
                let values = result?.assumingMemoryBound(to: Result.self)
                let numCols = Int(cols)

                if (values?.pointee.columnNames.count)! < numCols {
                    for j in 0..<numCols {
                        values?.pointee.columnNames.append(String(cString: (colName?[j])!))
                    }
                }

                var singleRow = [Any]()
                for i in 0..<numCols {
                    singleRow.append(String(cString: (colText?[i])!))
                }

                values?.pointee.results.append(singleRow)
                values?.pointee.returnedResult = true

                // Must return 0 for a successful execute
                return 0
            }, &result, &errmsg)

        if resultCode == SQLITE_OK {
            if result.returnedResult {
                onCompletion(.resultSet(ResultSet(SQLiteResultFetcher(titles: result.columnNames, rows: result.results))))
            } else {
                onCompletion(.successNoData)
            }
        } else if let errmsg = errmsg {
            onCompletion(.error(QueryError.databaseError(String(cString: errmsg))))
        }
    }
}
