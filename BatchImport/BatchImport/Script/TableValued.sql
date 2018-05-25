CREATE TYPE JCodeTableValued AS TABLE
(
Id UNIQUEIDENTIFIER,
JCode NVARCHAR(100),
JCode1 NVARCHAR(100),
JCode2 NVARCHAR(100),
RCode NVARCHAR(100),
CodeLevel INT,
ApplyNo NVARCHAR(100),
ProductNo VARCHAR(100),
AddOn DATETIME,
Remark NVARCHAR(100),
OrgID NVARCHAR(100),
Version INT,
IsDeleted BIT
)



SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		张帅
-- Create date: 20180525
-- Description:	使用表值参数批量导入码数据
-- =============================================
CREATE PROCEDURE SP_BatchJCodeByTableValued
    @TV AS JCodeTableValued READONLY
AS
    BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
        SET NOCOUNT ON;

        INSERT  INTO dbo.Track_Retrospect_JawasoftCode
                ( Id ,
                  JCode ,
                  JCode1 ,
                  JCode2 ,
	          --AppendCode ,
                  RCode ,
                  CodeLevel ,
                  ApplyNo ,
                  ProductNo ,
                  AddOn ,
	          --AddBy ,
	          --UpdateOn ,
	          --UpdateBy ,
	          --DeleteOn ,
	          --DeleteBy ,
                  Remark ,
                  OrgID ,
                  Version ,
                  IsDeleted
	            )
                SELECT  *
                FROM    @TV;

    END
GO
