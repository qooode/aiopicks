<h1 align="center">AIOPicks</h1>

<p align="center">
  <strong>AI-powered personalized recommendations for your next binge.</strong>
  <br />
  AIOPicks generates dynamic movie and TV show catalogs based on your Trakt watch history using advanced AI models from OpenRouter.
</p>

---

## ✨ What is AIOPicks?

AIOPicks revolutionizes content discovery by creating Netflix-style personalized catalogs for Stremio. Instead of browsing endless generic lists, AIOPicks analyzes your Trakt watch history and generates AI-powered recommendations tailored specifically to your viewing patterns and preferences.

Using advanced AI models from OpenRouter, AIOPicks creates dynamic catalogs that refresh automatically, ensuring you always have fresh, personalized content to discover.

## 🚀 Key Features

### 🤖 AI-Powered Personalization
- **Trakt Integration**: Analyzes your complete watch history, ratings, and viewing patterns
- **OpenRouter AI**: Leverages cutting-edge AI models (GPT-4, Claude, etc.) for intelligent recommendations
- **Dynamic Generation**: No hardcoded lists - everything is AI-generated based on your unique preferences
- **Privacy-Focused**: Your data stays yours - all processing happens on your instance

### 📊 User-Configurable Dynamic Catalogs

AIOPicks generates personalized catalogs with AI-generated names you won't know beforehand:

- **🌙 Your Late Night Thrillers** - *Perfect edge-of-your-seat content for late viewing*
- **🎭 Hidden Korean Gems You'll Love** - *Underrated content based on your patterns*
- **🔥 Weekend Binge Adventures** - *Series perfect for your weekend marathons*
- **✨ Critically Acclaimed Surprises** - *Award-winning content matching your taste*

### � Flexible Configuration
- **User-Configurable Count**: Choose 3-12 personalized catalogs
- **Custom Refresh Intervals**: Set how often catalogs refresh (daily, weekly, monthly)
- **AI Model Selection**: Choose from any OpenRouter model
- **Smart Caching**: Efficient storage prevents unnecessary AI API calls

### 🎯 Advanced Personalization
- **Viewing Pattern Analysis**: Learns from your binge habits and rating patterns
- **Genre Preferences**: Adapts to your favorite and avoided genres
- **Quality Standards**: Matches your preference for critically acclaimed vs. popular content
- **Discovery Balance**: Balances familiar comfort picks with adventurous new discoveries

## 🚀 Getting Started

### Prerequisites
- Trakt account with viewing history
- OpenRouter API key
- Docker (recommended) or Node.js 20+

### 1. Get Your API Keys

**Trakt API:**
1. Visit [Trakt API Settings](https://trakt.tv/oauth/applications/new)
2. Create a new application
3. Note your Client ID and Client Secret

**OpenRouter:**
1. Sign up at [OpenRouter.ai](https://openrouter.ai)
2. Generate an API key from your dashboard
3. Choose your preferred AI models

### 2. Deploy AIOPicks

**Docker (Recommended):**
```bash
docker run -d \
  --name aiopicks \
  -p 3000:3000 \
  -e TRAKT_CLIENT_ID=your_client_id \
  -e TRAKT_CLIENT_SECRET=your_client_secret \
  -e OPENROUTER_API_KEY=your_api_key \
  -e DEFAULT_MODEL=gpt-4o-mini \
  -e REFRESH_INTERVAL=86400 \
  aiopicks:latest
```

**Docker Compose:**
```yaml
version: '3.8'
services:
  aiopicks:
    build: .
    ports:
      - "3000:3000"
    environment:
      - TRAKT_CLIENT_ID=your_client_id
      - TRAKT_CLIENT_SECRET=your_client_secret
      - OPENROUTER_API_KEY=your_api_key
      - DEFAULT_MODEL=gpt-4o-mini
      - REFRESH_INTERVAL=86400
      - DATABASE_URL=postgresql://user:password@db:5432/aiopicks
    volumes:
      - aiopicks_data:/app/data
```

**Development Setup:**
```bash
git clone <repository-url>
cd aiopicks
npm install
cp .env.sample .env
# Edit .env with your API keys
npm run start:dev
```

### 3. Configure Your Setup
1. Open `http://localhost:3000/stremio/configure`
2. Connect your Trakt account
3. Configure AI model preferences
4. Set refresh intervals and catalog preferences (3-12 catalogs)
5. Install the generated addon URL in Stremio

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `TRAKT_CLIENT_ID` | Trakt API Client ID | - | ✅ |
| `TRAKT_CLIENT_SECRET` | Trakt API Client Secret | - | ✅ |
| `OPENROUTER_API_KEY` | OpenRouter API Key | - | ✅ |
| `DEFAULT_MODEL` | Default AI model | `gpt-4o-mini` | ✅ |
| `REFRESH_INTERVAL` | Catalog refresh interval (seconds) | `86400` (24h) | ✅ |
| `DATABASE_URL` | Database connection string | SQLite | ❌ |
| `REDIS_URL` | Redis connection string | - | ❌ |
| `PORT` | Server port | `3000` | ❌ |

### Supported AI Models
AIOPicks supports any model available on OpenRouter:
- `gpt-4o-mini` (recommended for cost efficiency)
- `gpt-4o` (best quality)
- `claude-3.5-sonnet` (excellent for creative recommendations)
- `llama-3.1-70b-instruct` (open-source alternative)
- And many more...

## 🏗️ Architecture

AIOPicks is built with a modern, scalable architecture:

- **Core Engine**: TypeScript-based recommendation engine
- **AI Integration**: OpenRouter API for model flexibility
- **Data Layer**: PostgreSQL/SQLite with Redis caching
- **Frontend**: Next.js configuration interface
- **API Server**: Express.js with Stremio protocol support

## 📈 How It Works

1. **Data Collection**: Securely fetches your Trakt watch history and ratings
2. **Pattern Analysis**: AI analyzes your viewing patterns, preferences, and habits
3. **Catalog Generation**: Creates user-configurable personalized recommendation catalogs
4. **Smart Caching**: Stores recommendations with configurable refresh intervals
5. **Stremio Integration**: Serves catalogs through standard Stremio protocol

## 🛠️ Development

### Available Scripts

- `npm run start:dev` - Start development server
- `npm run start:frontend:dev` - Start frontend development server
- `npm run build` - Build all packages
- `npm run test` - Run tests
- `npm run format` - Format code with Prettier

### Project Structure

```
packages/
├── core/           # TypeScript recommendation engine
├── frontend/       # Next.js configuration interface
└── server/         # Express.js API server
```

## ⚠️ Disclaimer

AIOPicks is a content discovery tool that generates recommendations based on your viewing history. It does not host, store, or distribute any copyrighted content. The recommendations are for discovery purposes only. Users are responsible for accessing content through legitimate means and complying with all applicable laws.

## 🙏 Credits

This project builds upon the foundational work of:
- **[Trakt.tv](https://trakt.tv)** for providing comprehensive viewing data APIs
- **[OpenRouter.ai](https://openrouter.ai)** for democratizing access to advanced AI models
- **[Stremio](https://stremio.com)** for the excellent media center platform

---

**Made for self-hosting enthusiasts who want Netflix-level personalized recommendations.**
