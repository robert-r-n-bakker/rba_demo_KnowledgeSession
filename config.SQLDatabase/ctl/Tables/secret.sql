CREATE TABLE [ctl].[secret] (
    [secret_name]  VARCHAR (100)  NOT NULL,
    [secret_value] VARCHAR (2000) NOT NULL,
    PRIMARY KEY CLUSTERED ([secret_name] ASC)
);


GO

