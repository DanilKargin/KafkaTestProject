FROM mcr.microsoft.com/dotnet/sdk:8.0
WORKDIR /src

# Сначала только csproj — чтобы restore кэшировался
COPY ["producer.csproj", "./"]
RUN dotnet restore "producer.csproj"

# Затем весь код
COPY . .

# Запуск без publish
ENTRYPOINT ["dotnet", "run", "--project", "producer.csproj", "-c", "Debug"]