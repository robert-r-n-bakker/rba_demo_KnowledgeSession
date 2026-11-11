import fabric.functions as fn
udf = fn.UserDataFunctions()

@udf.function()
def movie_label(budget: float, revenue: float, voteAverage: float, voteCount: int) -> str:
    if voteCount < 50:
        return "Te weinig stemmen"
    if budget and revenue and revenue >= 3 * budget:
        return "Blockbuster" if voteAverage >= 6.5 else "Kassucces"
    if budget and revenue and revenue < budget:
        return "Flop"
    if voteAverage >= 7.5:
        return "Verborgen parel"
    if voteAverage >= 8.5 and voteCount >= 500:
        return "Meesterwerk"
    return "Regulier"


@udf.connection(argName="sqlDB", alias="config")
@udf.function()
def add_to_watchlist(
    movieId: int,
    requestedBy: str,
    sqlDB: fn.FabricSqlConnection
) -> str:

    if movieId <= 0:
        return "Invalid movie ID."

    requestedBy = requestedBy.strip()

    if not requestedBy:
        return "Requested by cannot be empty."

    connection = sqlDB.connect()
    cursor = connection.cursor()

    try:
        query = """
            INSERT INTO dbo.watchlist
            (
                movie_id,
                requested_by,
                requested_at
            )
            VALUES (?, ?, SYSUTCDATETIME());
        """

        cursor.execute(
            query,
            (
                movieId,
                requestedBy
            )
        )

        connection.commit()

        return f"Movie {movieId} was added to the watchlist."

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()
