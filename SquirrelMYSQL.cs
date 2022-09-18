/*A class for storing and retrieving data from SQL
* SQL sructure
* [id:int] [tag|path:string] [subtag|identifier(name, age...):string] [data:string]
*/

using MySqlConnector;
using ScorpionConsoleReadWrite;

namespace ScorpionMySql
{
public class ScorpionSql:IDisposable
{
    private const string user_table = "users";
    private const string scorpion_formatted_table = "scfmt";

    //Create a schema with default tables
    public void scfmtMariaDbCreateSchema(string connection_string, string name)
    {
        try
        {
            using (var connection = new MySqlConnection(connection_string))
            {
                connection.Open();
                using (var command = new MySqlCommand($"CREATE SCHEMA {name};", connection))
                {                    
                    try
                    {
                        command.ExecuteNonQuery();
                        ConsoleWrite.writeSuccess("Schema created. Run the 'mysqlnew' command with a renewed connection string selecting the new schema in order to create the default formatted tables");
                    }
                    catch(System.Exception e){ ConsoleWrite.writeError(e.Message); }
                }
                connection.Close();
            }
        }
        catch(System.Exception e)
        {
            ConsoleWrite.writeError(e.Message);
        }
        finally
        {
            writeSuccessMessage();
        }
        return;
    }

    public /*Dictionary<string, string>*/ void scfmtSqlGet(string connection_string, string table, string path, string identifier, string data, string token, out Dictionary<string, string> returns)
    {
        //Get data from MySql in the generic format: [id:int] [tag|path:string] [subtag|identifier(name, age...):string] [data:string]
        //Returns this variable
        returns = new Dictionary<string, string>();

        try
        {
            using (var connection = new MySqlConnection(connection_string))
            {
                connection.Open();
                using (var command = new MySqlCommand(string.Format("SELECT * FROM {0} WHERE tag=@tag AND subtag LIKE @subtag AND data LIKE @data AND token=@token;", table), connection))
                {
                    command.Parameters.AddWithValue("tag", path);
                    command.Parameters.AddWithValue("subtag", string.Format("%{0}%", identifier));
                    command.Parameters.AddWithValue("data", string.Format("%{0}%", data));
                    command.Parameters.AddWithValue("token", token);

                    using (var reader = command.ExecuteReader())
                    {
                        if(reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                returns.Add(reader.GetString(2), reader.GetString(3));
                            }
                        }
                    }
                }
                connection.Close();
            }
        }
        catch(System.Exception e){ ConsoleWrite.writeError(e.Message); }
        finally
        {
            writeSuccessMessage();
        }
    }

    public void scfmtSqlSet(string connection_string, string table, string path, string identifier, string data, string token)
    {
        //Set data into MySql in the generic format: [id:int] [tag|path:string] [subtag|identifier(name, age...):string] [data:string]
        
        try
        {
            using (var connection = new MySqlConnection(connection_string))
            {
                connection.Open();
                using (var command = new MySqlCommand(string.Format("INSERT INTO {0} values(DEFAULT, @tag, @subtag, @data, @token)", table), connection))
                {
                    command.Parameters.AddWithValue("tag", path);
                    command.Parameters.AddWithValue("subtag", identifier);
                    command.Parameters.AddWithValue("data", data);
                    command.Parameters.AddWithValue("token", token);

                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            command.ExecuteNonQuery();
                        }
                        catch(System.Exception e){ ConsoleWrite.writeError(e.Message); }
                    }
                }
                connection.Close();
            }
        }
        catch(System.Exception e){ ConsoleWrite.writeError(e.Message); }
        finally
        {
            writeSuccessMessage();
        }
        return;
    }

    public void sqlfmtUpdate(string connection_string, string table, string path, string identifier, string update_with_data, string token)
    {
        try
        {
            using (var connection = new MySqlConnection(connection_string))
            {
                connection.Open();
                using (var command = new MySqlCommand(string.Format("UPDATE {0} SET data=@data WHERE tag=@tag AND subtag=@subtag AND token=@token", table), connection))
                {
                    command.Parameters.AddWithValue("tag", path);
                    command.Parameters.AddWithValue("subtag", identifier);
                    command.Parameters.AddWithValue("data", update_with_data);
                    command.Parameters.AddWithValue("token", token);

                    using (var reader = command.ExecuteReader())
                    {
                        try
                        {
                            command.ExecuteNonQuery();
                        }
                        catch(System.Exception e){ ConsoleWrite.writeError(e.Message); }
                    }
                }
                connection.Close();
            }
        }
        catch(System.Exception e)
        { 
            ConsoleWrite.writeError(e.Message);
        }
        finally
        {
            writeSuccessMessage();
        }
    }

    public void sqlfmtNew(string connection_string, string table_name)
    {
        //Creates a new generic data table with the following default format: [id:int] [tag|path:string] [subtag|identifier(name, age...):string] [data:string]
        
        try
        {
            using (var connection = new MySqlConnection(connection_string))
            {
                connection.Open();
                using (var command = new MySqlCommand(string.Format("CREATE TABLE {0} (id INT NOT NULL AUTO_INCREMENT, tag VARCHAR(128) NOT NULL, subtag VARCHAR(32) NOT NULL, data VARCHAR(2048) NULL, token VARCHAR(256) NOT NULL, PRIMARY KEY (id))", table_name), connection))
                {
                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    catch(System.Exception e){ ConsoleWrite.writeError(e.Message); }
                }
                connection.Close();
            }
        }
        catch(Exception e){ ConsoleWrite.writeError(e.Message); }
        finally
        {
            writeSuccessMessage();
        }
        return;
    }

    public void scfmtmySqlTest(string connection_string)
    {
            using (var connection = new MySqlConnection(connection_string))
            {
                try
                {
                    connection.Open();
                    using (var command = new MySqlCommand("SELECT json FROM module_users;", connection))
                    using (var reader = command.ExecuteReader())
                        while (reader.Read())
                            Console.WriteLine(reader.GetString(0));
                    connection.Close();        
                }
                catch(System.Exception e)
                {
                    ConsoleWrite.writeError(e.Message);
                }
                finally
                {
                    writeSuccessMessage();
                }
            }
            return;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        return;
    }

    private void writeSuccessMessage()
    {
        ScorpionConsoleReadWrite.ConsoleWrite.writeSuccess("MariaDB/MySQL OK.");
        return;
    }
}
}