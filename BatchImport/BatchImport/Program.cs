using E9.EA.BatchImport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BatchImport
{
    /// <summary>
    /// 两种批量导入方式
 	/// master分支提交
	/// test分支提   
    /// /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            string WebDir = System.AppDomain.CurrentDomain.BaseDirectory;//程序目录
            string sPath = WebDir + "/Files/";

            TabledValuedOperate operate = new TabledValuedOperate();
            //operate.ImportData(sPath, "AP20180524000008");

            //operate.ImportTableValued(sPath, "AP20180523000005");
            string[] applyNos = { "AP20180524000007", "AP20180523000005" };
            foreach (var item in applyNos)
            {
                operate.ImportTableValuedAsync(sPath, item);
            }
           

            Console.WriteLine("批量导入完成");
            Console.ReadKey();
        }
    }
}
