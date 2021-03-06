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

import XCTest
import SwiftKuery
@testable import SwiftKuerySQLite

#if os(Linux)
let tableSubquery = "tableSubqueryLinux"
#else
let tableSubquery = "tableSubqueryOSX"
#endif

class TestSubquery: XCTestCase {

    static var allTests: [(String, (TestSubquery) -> () throws -> Void)] {
        return [
            ("testSubquery", testSubquery),
        ]
    }

    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")

        let tableName = "tableSubquery"
    }

    func testSubquery() {
        let t = MyTable()
        let connection = createConnection()
        performTest(asyncTasks: { expectation in
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")

                cleanUp(table: t.tableName, connection: connection) { result in

                    executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(40), b integer)", connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                        let i1 = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        executeQuery(query: i1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                            var s = Select(from: t)
                                .where(t.b > (Select(t.b, from: t).where(t.b == 3)))
                            executeQuery(query: s, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                XCTAssertEqual(rows!.count, 4, "SELECT returned wrong number of rows: \(rows!.count) instead of 4")

                                s = Select(from: t)
                                    .where(exists(Select(t.b, from: t).where(t.b == 10)))
                                executeQuery(query: s, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                    XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")

                                    s = Select(from: t)
                                        .where(8.in(1,6,8))
                                    executeQuery(query: s, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                        XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")

                                        s = Select(from: t)
                                            .having("apple".notIn("plum"))
                                            .group(by: t.a, t.b)
                                        executeQuery(query: s, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                            XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")

                                            s = Select(from: t)
                                                .group(by: t.a, t.b)
                                                .having(exists(Select(t.b, from: t).where(t.b == 17)))
                                            executeQuery(query: s, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                                XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")

                                                s = Select(from: t)
                                                    .where(notExists(Select(t.b, from: t).where(t.b == 8)))
                                                executeQuery(query: s, connection: connection) { result, rows in
                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                    XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                                    XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }

}
