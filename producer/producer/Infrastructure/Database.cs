using Npgsql;

namespace producer.Infrastructure
{
    public class Database
    {
        private readonly string _connectionString;

        public Database(string connectionString)
        {
            _connectionString = connectionString;
        }

        public NpgsqlConnection CreateConnection() => new(_connectionString);

        public async Task<NpgsqlConnection> OpenAsync(CancellationToken ct = default)
        {
            var conn = CreateConnection();
            await conn.OpenAsync(ct);
            return conn;
        }
    }
}
