CREATE TABLE [dbo].[watchlist] (
    [watchlist_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [movie_id]     BIGINT        NOT NULL,
    [requested_by] VARCHAR (255) NOT NULL,
    [requested_at] DATETIME2 (7) CONSTRAINT [DF_watchlist_requested_at] DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([watchlist_id] ASC)
);


GO

