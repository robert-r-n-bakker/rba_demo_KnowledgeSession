CREATE TABLE [ctl].[run_log] (
    [run_id]        VARCHAR (50)   NULL,
    [endpoint_name] VARCHAR (100)  NULL,
    [pages_loaded]  INT            NULL,
    [status]        VARCHAR (20)   NULL,
    [started_at]    DATETIME2 (7)  NULL,
    [ended_at]      DATETIME2 (7)  NULL,
    [error_message] VARCHAR (4000) NULL
);


GO

