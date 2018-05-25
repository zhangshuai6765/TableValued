using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace E9.EA.BatchImport
{
    public class BatchImportUtil
    {
        #region 属性定义
        /// <summary>
        /// 与数据库连接字符串
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// 连接超时时间
        /// </summary>
        public int ConnectTimeout { get; set; }

        public SqlTransaction _tran;

        public  SqlConnection conn; 
        #endregion

        #region 构造函数
        /// <summary>
        ///  构造函数
        /// </summary>
        /// <param name="connStr">数据库连接字符串</param>
        public BatchImportUtil(string connStr)
        {
            this.ConnectionString = connStr;
            this.ConnectTimeout = 60 * 10;
            init();
        }

        /// <summary>
        ///  构造函数
        /// </summary>
        /// <param name="connStr">数据库连接字符串</param>
        /// <param name="ConnTimeout">超时时间</param>
        public BatchImportUtil(string connStr, int ConnTimeout)
        {
            this.ConnectionString = connStr;
            this.ConnectTimeout = ConnTimeout;
            init();
        }

        public BatchImportUtil(SqlConnection conn, SqlTransaction tran)
        {
            this.conn = conn;
            this._tran = tran;
        }
        #endregion

        #region 初始化
        /// <summary>
        /// 初始化
        /// </summary>
        private void init() {
            conn = new SqlConnection(ConnectionString); 
        }
        #endregion

        #region 开始事务
        /// <summary>
        /// 开始事务
        /// </summary>
        public void BeginTran()
        {
            if (this.conn.State == ConnectionState.Closed)
            {
                this.conn.Open();
            }
            _tran = this.conn.BeginTransaction();
        }
        #endregion

        #region 执行存储过程
        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="procedureName"></param>
        /// <returns></returns>
        public int ExecuteTran(string procedureName,SqlParameter[] lstParam = null)
        {
            int retval = 0;
            try
            {
                SqlCommand cmd = new SqlCommand(procedureName, conn, _tran);
                cmd.CommandTimeout = ConnectTimeout;
                cmd.CommandType = GetCommandType(procedureName);

                if (lstParam != null)
                {
                    foreach (SqlParameter param in lstParam)
                    {
                        cmd.Parameters.Add(param);
                    }
                }

                retval = cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                OnSqlException(ex, procedureName);
            }

            return retval;
        }
        #endregion

        #region 执行存储过程
        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="procedureName"></param>
        /// <returns></returns>
        public int Execute(string procedureName, SqlParameter[] lstParam = null)
        {
            var _DBConn = new SqlConnection(ConnectionString);
            _DBConn.Open();

            int retval = 0;
            try
            {
                SqlCommand cmd = new SqlCommand(procedureName, _DBConn);
                cmd.CommandTimeout = ConnectTimeout;
                cmd.CommandType = GetCommandType(procedureName);

                if (lstParam != null)
                {
                    foreach (SqlParameter param in lstParam)
                    {
                        cmd.Parameters.Add(param);
                    }
                }

                retval = cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                OnSqlException(ex, procedureName);
            }
            finally
            {
                _DBConn.Close();
            }

            return retval;
        }
        #endregion

        #region 批量导入
        /// <summary>
        /// 批量导入
        /// </summary>
        /// <param name="data"></param>
        /// <param name="destTableName"></param>
        /// <param name="columnMapping"></param>
        /// <param name="options"></param>
        public void DoBatchImportTran(DataTable data, string destTableName, List<SqlBulkCopyColumnMapping> columnMapping, SqlBulkCopyOptions options)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, options, _tran))
            {
                bulkCopy.DestinationTableName = destTableName;
                if (columnMapping != null)
                {
                    columnMapping.ForEach(m => { bulkCopy.ColumnMappings.Add(m); });
                }

                try
                {
                    bulkCopy.WriteToServer(data);
                }
                catch (DbException ex)
                {
                    OnSqlException(ex, "批量导入数据： " + destTableName, null);
                }
            }
        }

        public void DoBatchImportTran(DataTable data,string sql)
        {
            int recordCount = 0;
            sql = sql.Trim();
            SqlCommand cmd = new SqlCommand(sql, this.conn, _tran);
            SqlDataAdapter adapter = new SqlDataAdapter(cmd);
            SqlCommandBuilder builder = new SqlCommandBuilder(adapter);
            DataSet ds = new DataSet();
            adapter.Fill(ds, "batchTable");
            DataTable table = ds.Tables["batchTable"];
            //将新数据传到对应的datatable中  
            foreach (DataRow drNew in data.Rows)
            {
                DataRow row = table.NewRow();
                foreach (DataColumn dc in table.Columns)
                {
                    row[dc.ColumnName] = drNew[dc.ColumnName];
                }
                table.Rows.Add(row);
            }

            //更新数据库  
            adapter.Update(table);
        }
        #endregion

        #region 提交事务
        /// <summary>
        /// 提交事务
        /// </summary>
        public void CommitTran()
        {
            _tran.Commit();
            this.conn.Close();
            _tran.Dispose();
        }
        #endregion

        #region 回滚事务
        /// <summary>
        /// 回滚事务
        /// </summary>
        public void RollBack()
        {
            _tran.Rollback();
            this.conn.Close();
            _tran.Dispose();
        }
        #endregion

        #region 批量导入
        /// <summary>
        /// 批量导入
        /// </summary>
        /// <param name="data"></param>
        /// <param name="destTableName"></param>
        /// <param name="columnMapping"></param>
        /// <param name="options"></param>
        public void DoBatchImport(DataTable data, string destTableName, List<SqlBulkCopyColumnMapping> columnMapping, SqlBulkCopyOptions options)
        {
            var _DBConn = new SqlConnection(ConnectionString);
            _DBConn.Open();


            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(_DBConn, options, null))
            {
                bulkCopy.DestinationTableName = destTableName;
                bulkCopy.BulkCopyTimeout = 400;
                if (columnMapping != null)
                {
                    columnMapping.ForEach(m => { bulkCopy.ColumnMappings.Add(m); });
                }

                try
                {
                    bulkCopy.WriteToServer(data);
                }
                catch (DbException ex)
                {
                    OnSqlException(ex, "批量导入数据： " + destTableName, null);
                }
                finally
                {
                    _DBConn.Close();
                }
            }
        }

        public int DoBatchImport(DataTable data, string sql)
        {
            int recordCount = 0;
            sql = sql.Trim();
            SqlCommand cmd = new SqlCommand(sql, this.conn);
            SqlDataAdapter adapter = new SqlDataAdapter(cmd);
            SqlCommandBuilder builder = new SqlCommandBuilder(adapter);
            DataSet ds = new DataSet();
            adapter.Fill(ds, "batchTable");
            DataTable table = ds.Tables["batchTable"];
            //将新数据传到对应的datatable中  
            foreach (DataRow drNew in data.Rows)
            {
                DataRow row = table.NewRow();
                foreach (DataColumn dc in table.Columns)
                {
                    row[dc.ColumnName] = drNew[dc.ColumnName];
                }
                table.Rows.Add(row);
            }

            //更新数据库  
            recordCount = adapter.Update(table);
            return recordCount;
        }
        #endregion

        #region 取得DataSet
        /// <summary>
        /// 取得DataSet
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DataSet ExecuteDataSet(string query, params DbParameter[] parameters)
        {
            query = query.Trim();
            SqlCommand cmd = new SqlCommand(query, this.conn);
            cmd.CommandTimeout = this.ConnectTimeout;
            cmd.CommandType = GetCommandType(query);
            if (parameters != null)
            {
                for (int i = 0; i <= parameters.Length - 1; i++)
                {
                    cmd.Parameters.Add(parameters[i]);
                }
            }

            DataSet ds = new DataSet();
            try
            {
                this.conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(ds);
            }
            catch (SqlException ex)
            {
                OnSqlException(ex, query, parameters);
            }
            finally
            {
                this.conn.Close();
            }

            return ds;
        }

        public DataSet ExecuteDataSet(string sqlStr)
        {
            sqlStr = sqlStr.Trim();
            SqlCommand cmd = new SqlCommand(sqlStr, this.conn);
            cmd.CommandTimeout = this.ConnectTimeout;
            cmd.CommandType = CommandType.Text;
            DataSet ds = new DataSet();
            try
            {
                this.conn.Open();
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(ds);
            }
            catch(Exception ex)
            {
                new ApplicationException(ex.Message, ex);
            }
            finally
            {
                this.conn.Close();
            }
            return ds;
        }
        #endregion
        public int Execute(string sql)
        {
            sql = sql.Trim();
            SqlCommand cmd = new SqlCommand(sql, this.conn);
            cmd.CommandTimeout = this.ConnectTimeout;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = sql;
            cmd.Transaction = _tran;
            int i = -1;
            try
            {   if(conn.State==ConnectionState.Closed)
                     this.conn.Open();
                i=cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                OnSqlException(ex, sql,null);
            }
            finally
            {
                this.conn.Close();
            }

            return i;
        }

        public object ExecuteScalar(string sql)
        {
            sql = sql.Trim();
            SqlCommand cmd = new SqlCommand(sql, this.conn);
            cmd.CommandTimeout = this.ConnectTimeout;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = sql;
            cmd.Transaction = _tran;
            object obj = new object();
            try
            {
                if (conn.State == ConnectionState.Closed)
                    this.conn.Open();
                obj = cmd.ExecuteScalar();
            }
            catch (SqlException ex)
            {
                OnSqlException(ex, sql, null);
            }
            finally
            {
                this.conn.Close();
            }

            return obj;
        }

        public int ExecuteByParams(string sql ,SqlParameter[] Par)
        {
            sql = sql.Trim();
            SqlCommand cmd = new SqlCommand(sql, this.conn);
            cmd.CommandTimeout = this.ConnectTimeout;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = sql;
            cmd.Transaction = _tran;
            int i = -1;
            for (int j = 0; j <= Par.GetUpperBound(0); j++)
            {
                cmd.Parameters.Add(Par[j]);
            }
            try
            {
                if(conn.State==ConnectionState.Closed)
                     this.conn.Open();
                i=cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                OnSqlException(ex, sql, null);
            }
            finally
            {
                this.conn.Close();
            }

            return i;
        }
     
       /// <summary>
       /// 批量插入到表
       /// </summary>
       /// <param name="dt"></param>
       /// <param name="TableName"></param>
        public void ExecuteBulk(DataTable dt,string TableName)
        {
            SqlBulkCopy bulkCopy = new SqlBulkCopy(this.conn);
            bulkCopy.DestinationTableName = TableName;
            bulkCopy.BatchSize = dt.Rows.Count;
            try
            {
                if (conn.State == ConnectionState.Closed)
                    this.conn.Open();
                if (dt != null && dt.Rows.Count != 0)
                    bulkCopy.WriteToServer(dt);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                this.conn.Close();
                if (bulkCopy != null)
                    bulkCopy.Close();
            }
        }
        #region 取得DataSet
        /// <summary>
        /// 取得DataSet
        /// </summary>
        /// <param name="query"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DataSet ExecuteDataSetTran(string query, params DbParameter[] parameters)
        {
            query = query.Trim();
            SqlCommand cmd = new SqlCommand(query, this.conn);
            cmd.CommandTimeout = this.ConnectTimeout;
            cmd.CommandType = GetCommandType(query);
            cmd.Transaction = this._tran;
            if (parameters != null)
            {
                for (int i = 0; i <= parameters.Length - 1; i++)
                {
                    cmd.Parameters.Add(parameters[i]);
                }
            }

            DataSet ds = new DataSet();
            try
            {
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                da.Fill(ds);
            }
            catch (SqlException ex)
            {
                OnSqlException(ex, query, parameters);
            }

            return ds;
        }
        #endregion

        public CommandType GetCommandType(string sSqlText)
        {
            sSqlText = sSqlText.TrimStart();
            if (sSqlText.StartsWith("INSERT", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("UPDATE", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("SET", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("DROP", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("DELETE", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("EXEC ", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("ALTER ", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("CREATE ", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("WITH", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("BACKUP", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("RESTORE", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("IF", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("Using", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("SELECT", StringComparison.InvariantCultureIgnoreCase)
                || sSqlText.StartsWith("DECLARE", StringComparison.InvariantCultureIgnoreCase))
            {
                return CommandType.Text;
            }
            else
            {
                return CommandType.StoredProcedure;
            }
        }


        #region 表值参数

        public int BulkTableValuedToDB(DataTable dt,string sql,string tableValuedName,string paramsName)
        {
            sql = sql.Trim();
            SqlCommand cmd = new SqlCommand(sql, this.conn);
            cmd.CommandTimeout = this.ConnectTimeout;
            //cmd.CommandType = CommandType.Text;
            //cmd.CommandText = sql;
            SqlParameter catParam = cmd.Parameters.AddWithValue(paramsName, dt);
            catParam.SqlDbType = SqlDbType.Structured;
            cmd.CommandType = GetCommandType(sql);
            catParam.TypeName = tableValuedName;

            int i = -1;
            try
            {
                if (conn.State == ConnectionState.Closed)
                    this.conn.Open();
                i = cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                OnSqlException(ex, sql, null);
            }
            finally
            {
                this.conn.Close();
            }

            return i;
        }

        public int BulkTableValuedToDB(string sql,SqlParameter[] lstParam = null)
        {
            sql = sql.Trim();
            SqlCommand cmd = new SqlCommand(sql, this.conn);
            cmd.CommandTimeout = this.ConnectTimeout;
            //SqlParameter catParam = cmd.Parameters.AddWithValue(tableValuedName, dt);
            //catParam.SqlDbType = SqlDbType.Structured;
            //catParam.TypeName = tableValuedName;
            cmd.CommandType = GetCommandType(sql);
           

            if (lstParam != null)
            {
                foreach (SqlParameter param in lstParam)
                {
                    cmd.Parameters.Add(param);
                }
            }


            int i = -1;
            try
            {
                if (conn.State == ConnectionState.Closed)
                    this.conn.Open();
                i = cmd.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                OnSqlException(ex, sql, null);
            }
            finally
            {
                this.conn.Close();
            }

            return i;
        }

        #endregion


        #region 异常处理
        /// <summary>
        /// 异常处理
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="sSql"></param>
        /// <param name="parameters"></param>
        private void OnSqlException(DbException ex, string sSql, params DbParameter[] parameters)
        {
            var outSQL = sSql;
            if (parameters != null)
            {
                foreach (var p in parameters)
                    outSQL += string.Format(" {0}='{1}',", p.ParameterName, p.Value);
            }
            if (outSQL.EndsWith(","))
                outSQL = outSQL.Remove(outSQL.Length - 1);

            throw new ApplicationException("DB Error：" + ex.Message, ex);
        }
        #endregion
    }
}
