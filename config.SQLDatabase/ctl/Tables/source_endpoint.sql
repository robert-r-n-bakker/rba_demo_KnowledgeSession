CREATE TABLE [ctl].[source_endpoint] (
    [endpoint_id]    INT           IDENTITY (1, 1) NOT NULL,
    [source_system]  VARCHAR (50)  NOT NULL,
    [endpoint_name]  VARCHAR (100) NOT NULL,
    [relative_url]   VARCHAR (500) NOT NULL,
    [is_paged]       BIT           NOT NULL,
    [max_pages]      INT           DEFAULT ((1)) NOT NULL,
    [target_folder]  VARCHAR (200) NOT NULL,
    [load_type]      VARCHAR (20)  DEFAULT ('full') NOT NULL,
    [is_enabled]     BIT           DEFAULT ((1)) NOT NULL,
    [last_watermark] DATE          NULL,
    PRIMARY KEY CLUSTERED ([endpoint_id] ASC)
);


GO

