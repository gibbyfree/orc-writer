import pyarrow as pa

schemas = {
    "name.basics": pa.schema(
        [
            pa.field("nconst", pa.string(), nullable=False),
            pa.field("primaryName", pa.string(), nullable=False),
            pa.field("birthYear", pa.string(), nullable=True),
            pa.field("deathYear", pa.string(), nullable=True),
            pa.field("primaryProfession", pa.string(), nullable=False),
            pa.field("knownForTitles", pa.string(), nullable=False),
        ]
    ),
    "title.akas": pa.schema(
        [
            pa.field("titleId", pa.string(), nullable=False),
            pa.field("ordering", pa.int32(), nullable=False),
            pa.field("title", pa.string(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("language", pa.string(), nullable=False),
            pa.field("types", pa.string(), nullable=False),
            pa.field("attributes", pa.string(), nullable=False),
            pa.field("isOriginalTitle", pa.int32(), nullable=False),
        ]
    ),
    "title.principals": pa.schema(
        [
            pa.field("tconst", pa.string(), nullable=False),
            pa.field("ordering", pa.int32(), nullable=False),
            pa.field("nconst", pa.string(), nullable=False),
            pa.field("category", pa.string(), nullable=False),
            pa.field("job", pa.string(), nullable=False),
            pa.field("characters", pa.string(), nullable=False),
        ]
    ),
    "title.ratings": pa.schema(
        [
            pa.field("tconst", pa.string(), nullable=False),
            pa.field("averageRating", pa.float64(), nullable=False),
            pa.field("numVotes", pa.int64(), nullable=False),
        ]
    ),
    "title.crew": pa.schema(
        [
            pa.field("tconst", pa.string(), nullable=False),
            pa.field("directors", pa.string(), nullable=False),
            pa.field("writers", pa.string(), nullable=False),
        ]
    ),
    "title.basics": pa.schema(
        [
            pa.field("tconst", pa.string(), nullable=False),
            pa.field("titleType", pa.string(), nullable=False),
            pa.field("primaryTitle", pa.string(), nullable=False),
            pa.field("originalTitle", pa.string(), nullable=False),
            pa.field("isAdult", pa.int32(), nullable=False),
            pa.field("startYear", pa.int32(), nullable=True),
            pa.field("endYear", pa.string(), nullable=True),
            pa.field("runtimeMinutes", pa.string(), nullable=True),
            pa.field("genres", pa.string(), nullable=False),
        ]
    ),
    "title.episode": pa.schema(
        [
            pa.field("tconst", pa.string(), nullable=False),
            pa.field("parentTconst", pa.string(), nullable=False),
            pa.field("seasonNumber", pa.int32(), nullable=True),
            pa.field("episodeNumber", pa.int32(), nullable=True),
        ]
    ),
}
