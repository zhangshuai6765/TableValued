using E9.EA.BatchImport;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BatchImport
{
    public class TabledValuedOperate
    {
        private DataTable ReadFile(string filePathName)
        {
            string fileName = filePathName.Replace(".txt", "");
            fileName = fileName.Substring(fileName.LastIndexOf("\\") + 1);
            string[] fileNamePart = fileName.Split('_');
            string applyNo = fileNamePart[0];
            //IRepository<Apply, Guid> repository = new Repository<Apply, Guid>();
            //Apply entity = repository.TrackEntities.Where(n => n.ApplyNo == applyNo).FirstOrDefault();
            string codeLevel = fileNamePart[fileNamePart.Length - 1];
            string productNo = string.Empty;
            string rcode = string.Empty;
            //string orgId = entity.OrgID;
            string orgId = "0779dc81-7bda-11e7-b412-080027687380";
            //if (entity.Product != null)
            //{
            //    productNo = entity.Product.ProductNO;
            //    string productShortNo = entity.Product.ShortNo;
            //    rcode = productShortNo + codeLevel;
            //}

            DataTable dt = CreatTable();
            StreamReader sr = File.OpenText(filePathName);
            string nextLine;
            while ((nextLine = sr.ReadLine()) != null)
            {
                if (string.IsNullOrEmpty(nextLine))
                    continue;
                DataRow dr = dt.NewRow();
                string[] dataItem = nextLine.Split(',');
                switch (dataItem.Length)
                {
                    case 2:
                        //jcode
                        dr["Id"] = Guid.NewGuid();
                        dr["JCode"] = dataItem[1];
                        dr["JCode1"] = "";
                        dr["JCode2"] = "";
                        break;
                    case 4:
                        //jcode、jcode1
                        dr["Id"] = Guid.NewGuid();
                        dr["JCode"] = dataItem[1];
                        dr["JCode1"] = dataItem[3];
                        dr["JCode2"] = "";
                        break;
                    case 6:
                        //jcode、jcode1 、jcode2
                        dr["Id"] = Guid.NewGuid();
                        dr["JCode"] = dataItem[1];
                        dr["JCode1"] = dataItem[3];
                        dr["JCode2"] = dataItem[5];
                        break;
                    default:
                        break;
                }

                dr["AppendCode"] = "";
                dr["RCode"] = rcode;
                dr["CodeLevel"] = Convert.ToInt32(codeLevel);
                dr["ApplyNo"] = applyNo;
                dr["ProductNo"] = productNo;

                dr["AddOn"] = DateTime.Now;
                dr["AddBy"] = "";
                dr["UpdateOn"] = DBNull.Value;
                dr["UpdateBy"] = "";
                dr["DeleteOn"] = DBNull.Value;
                dr["DeleteBy"] = "";

                dr["Remark"] = "";
                dr["OrgID"] = orgId;
                dr["Version"] = DBNull.Value;
                dr["IsDeleted"] = false;

                dt.Rows.Add(dr);
            }
            sr.Close();

            return dt;
        }

        private DataTable ReadFile2(string filePathName)
        {
            string fileName = filePathName.Replace(".txt", "");
            fileName = fileName.Substring(fileName.LastIndexOf("\\") + 1);
            string[] fileNamePart = fileName.Split('_');
            string applyNo = fileNamePart[0];
            //IRepository<Apply, Guid> repository = new Repository<Apply, Guid>();
            //Apply entity = repository.TrackEntities.Where(n => n.ApplyNo == applyNo).FirstOrDefault();
            string codeLevel = fileNamePart[fileNamePart.Length - 1];
            string productNo = string.Empty;
            string rcode = string.Empty;
            //string orgId = entity.OrgID;
            string orgId = "0779dc81-7bda-11e7-b412-080027687380";
            //if (entity.Product != null)
            //{
            //    productNo = entity.Product.ProductNO;
            //    string productShortNo = entity.Product.ShortNo;
            //    rcode = productShortNo + codeLevel;
            //}

            DataTable dt = CreatTable2();
            StreamReader sr = File.OpenText(filePathName);
            string nextLine;
            while ((nextLine = sr.ReadLine()) != null)
            {
                if (string.IsNullOrEmpty(nextLine))
                    continue;
                DataRow dr = dt.NewRow();
                string[] dataItem = nextLine.Split(',');
                switch (dataItem.Length)
                {
                    case 2:
                        //jcode
                        dr["Id"] = Guid.NewGuid();
                        dr["JCode"] = dataItem[1];
                        dr["JCode1"] = "";
                        dr["JCode2"] = "";
                        break;
                    case 4:
                        //jcode、jcode1
                        dr["Id"] = Guid.NewGuid();
                        dr["JCode"] = dataItem[1];
                        dr["JCode1"] = dataItem[3];
                        dr["JCode2"] = "";
                        break;
                    case 6:
                        //jcode、jcode1 、jcode2
                        dr["Id"] = Guid.NewGuid();
                        dr["JCode"] = dataItem[1];
                        dr["JCode1"] = dataItem[3];
                        dr["JCode2"] = dataItem[5];
                        break;
                    default:
                        break;
                }

                //dr["AppendCode"] = "";
                dr["RCode"] = rcode;
                dr["CodeLevel"] = Convert.ToInt32(codeLevel);
                dr["ApplyNo"] = applyNo;
                dr["ProductNo"] = productNo;

                dr["AddOn"] = DateTime.Now;
                //dr["AddBy"] = "";
                //dr["UpdateOn"] = DBNull.Value;
                //dr["UpdateBy"] = "";
                //dr["DeleteOn"] = DBNull.Value;
                //dr["DeleteBy"] = "";

                dr["Remark"] = "";
                dr["OrgID"] = orgId;
                dr["Version"] = DBNull.Value;
                dr["IsDeleted"] = false;

                dt.Rows.Add(dr);
            }
            sr.Close();

            return dt;
        }

        private DataTable CreatTable()
        {
            DataTable dt = new DataTable("Track_Retrospect_JawasoftCode");
            dt.Columns.Add("Id", Type.GetType("System.Guid"));
            dt.Columns.Add("JCode", Type.GetType("System.String"));
            dt.Columns.Add("JCode1", Type.GetType("System.String"));
            dt.Columns.Add("JCode2", Type.GetType("System.String"));
            dt.Columns.Add("AppendCode", Type.GetType("System.String"));
            dt.Columns.Add("RCode", Type.GetType("System.String"));
            dt.Columns.Add("CodeLevel", Type.GetType("System.Int32"));
            dt.Columns.Add("ApplyNo", Type.GetType("System.String"));
            dt.Columns.Add("ProductNo", Type.GetType("System.String"));
            dt.Columns.Add("AddOn", Type.GetType("System.DateTime"));
            dt.Columns.Add("AddBy", Type.GetType("System.String"));
            dt.Columns.Add("UpdateOn", Type.GetType("System.DateTime"));
            dt.Columns.Add("UpdateBy", Type.GetType("System.String"));
            dt.Columns.Add("DeleteOn", Type.GetType("System.DateTime"));
            dt.Columns.Add("DeleteBy", Type.GetType("System.String"));
            dt.Columns.Add("Remark", Type.GetType("System.String"));
            dt.Columns.Add("OrgID", Type.GetType("System.String"));
            dt.Columns.Add("Version", Type.GetType("System.Int32"));
            dt.Columns.Add("IsDeleted", Type.GetType("System.Boolean"));
            return dt;
        }

        private DataTable CreatTable2()
        {
            DataTable dt = new DataTable("Track_Retrospect_JawasoftCode");
            dt.Columns.Add("Id", Type.GetType("System.Guid"));
            dt.Columns.Add("JCode", Type.GetType("System.String"));
            dt.Columns.Add("JCode1", Type.GetType("System.String"));
            dt.Columns.Add("JCode2", Type.GetType("System.String"));
            dt.Columns.Add("RCode", Type.GetType("System.String"));
            dt.Columns.Add("CodeLevel", Type.GetType("System.Int32"));
            dt.Columns.Add("ApplyNo", Type.GetType("System.String"));
            dt.Columns.Add("ProductNo", Type.GetType("System.String"));
            dt.Columns.Add("AddOn", Type.GetType("System.DateTime"));
            dt.Columns.Add("Remark", Type.GetType("System.String"));
            dt.Columns.Add("OrgID", Type.GetType("System.String"));
            dt.Columns.Add("Version", Type.GetType("System.Int32"));
            dt.Columns.Add("IsDeleted", Type.GetType("System.Boolean"));
            return dt;
        }


        public void ImportData(string sPath,string ApplyNo)
        {
            string dicPath = sPath + ApplyNo;
            if (Directory.Exists(dicPath))
            {
                string[] files = Directory.GetFiles(dicPath);
                if (files.Length > 0)
                {
                    DataTable dt = ReadFile(files[0]);

                    string connStr = System.Configuration.ConfigurationManager.ConnectionStrings["ctpconn"].ConnectionString;
                    BatchImportUtil import = new BatchImportUtil(connStr);
                    import.DoBatchImport(dt, "Track_Retrospect_JawasoftCode", null, System.Data.SqlClient.SqlBulkCopyOptions.Default);
                }
            }
        }


        public void ImportTableValued(string sPath, string ApplyNo)
        {
            string dicPath = sPath + ApplyNo;
            if (Directory.Exists(dicPath))
            {
                string[] files = Directory.GetFiles(dicPath);
                if (files.Length > 0)
                {
                    DataTable dt = ReadFile2(files[0]);

                    string connStr = System.Configuration.ConfigurationManager.ConnectionStrings["ctpconn"].ConnectionString;
                    BatchImportUtil import = new BatchImportUtil(connStr);
                    SqlParameter[] param = new SqlParameter[1];
                    SqlParameter p = new SqlParameter("@TV", SqlDbType.Structured);
                    p.Value = dt;
                    p.TypeName = "dbo.JCodeTableValued";
                    param[0] = p;

                    import.BulkTableValuedToDB("SP_BatchJCodeByTableValued", param);
                }
            }
            
        }

        public async void ImportTableValuedAsync(string sPath, string ApplyNo)
        {
            await Task.Run(() =>
            {
                ImportTableValued(sPath, ApplyNo);
            });
        }
    }
}
