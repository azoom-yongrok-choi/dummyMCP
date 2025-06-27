import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { z } from 'zod'
import {BigQuery} from '@google-cloud/bigquery'
import OpenAI from 'openai'
import 'dotenv/config'
import { extractJsonFromLLMResponse, buildQuery } from './utils'

const bigquery = new BigQuery({
  keyFilename: 'gcp-key.json',
  projectId: 'azoom-yongrok-choi',
})

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
})

const OutputSchema = z.object({
  country_name: z.string().optional(),
  latitude: z.number().optional(),
  longitude: z.number().optional(),
  date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
});

// Create server instance
const server = new McpServer({
  name: 'Dummy BigQuery MCP',
  version: '1.0.0',
})

// Prompt 
const parseCovidQuery = async (input: string) => {
  const systemMessage = `
You are a multilingual query parser for COVID-19 data.

Your task is to extract structured JSON data from natural language queries.
The input may be in any language, but the output must use English field names and English values only.

Only include the following fields if they are explicitly mentioned:
- country_name (string, in English)
- latitude (number)
- longitude (number)
- date (string in YYYY-MM-DD format)

Field rules:
- For country_name:
  You may infer the country_name from city or region names using your knowledge. (e.g., "Seoul" → "South Korea", "Tokyo" → "Japan")
- For all other fields (latitude, longitude, date):
  Only include them if the user explicitly mentioned them. Never guess, infer, or use default values for these fields.

General rules:
- Do not include any field that was not clearly stated or, for country_name, inferred from city/region.
- Output a valid JSON object and nothing else.
`;

  const completion = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      { role: "system", content: systemMessage },
      { role: "user", content: input },
    ],
    temperature: 0.2,
  });

  return completion.choices[0].message.content ?? "";
}

server.prompt(
  "parse-covid-query",
  {
    input: z.string().describe("Natural language query for COVID-19 data: country_name, latitude, longitude, date"),
  },
  async ({ input }) => {
    const rawOutput = await parseCovidQuery(input);
    const json = extractJsonFromLLMResponse(rawOutput, OutputSchema);

    if (!json) {
      return {
        messages: [
          {
            role: 'assistant',
            content: {
              type: 'text',
              text: '[ERROR] LLM response is not a valid JSON: ' + rawOutput,
            },
          },
        ],
      };
    } else {
    return {
      messages: [
        {
          role: 'assistant',
          content: {
            type: 'text',
            text: JSON.stringify(json, null, 2),
          },
        },
      ],
    };
  }}
);

// Tool 
server.tool(
  'search-covid-list',
  'Search for data in covid19_open_data with user-specified limit (1~30).',
  { limit: z.number().min(1).max(30) },
  async (args) => {
    try {
      const limit = args.limit
      const query = `SELECT * FROM \`covid_dummy.covid19_open_data\` LIMIT ${limit}`
      const [rows] = await bigquery.query({ query })

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(rows, null, 2),
          },
        ],
      }
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: '[ERROR] ' + (error instanceof Error ? error.message : String(error)),
          },
        ],
      }
    }
  }
)

server.tool(
  "parse-covid-json",
  "Parse a natural language into structured JSON for COVID-19 data.",
  {
    input: z.string().describe("Natural language for structured JSON: country_name, latitude, longitude, date"),
  },
  async ({ input }) => {
    const rawOutput = await parseCovidQuery(input);
    const json = extractJsonFromLLMResponse(rawOutput, OutputSchema);

    if (!json) {
      return {
        content: [
          {
            type: 'text',
            text: '[ERROR] LLM response is not a valid JSON: ' + rawOutput,
          },
        ],
      };
    } else {
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(json, null, 2),
        },
      ],
    };
  }}
);

server.tool(
  'query-covid-data',
  'Query covid19_open_data with structured parameters.',
  OutputSchema.shape,
  async (args) => {
    const { query, params } = buildQuery('covid_dummy.covid19_open_data', args);
    try {
      const [rows] = await bigquery.query({
        query,
        params
      });
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(rows, null, 2),
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: '[ERROR] ' + (error instanceof Error ? error.message : String(error)),
          },
        ],
      };
    }
  }
);

server.tool(
  'nl-covid-query',
  'Query covid19_open_data with natural language query.',
  { input: z.string().describe("Natural language query for COVID-19 data: country_name, latitude, longitude, date") },
  async ({ input }) => {
    const rawOutput = await parseCovidQuery(input);
    const json = extractJsonFromLLMResponse(rawOutput, OutputSchema);

    if (!json) {
      return {
        content: [
          {
            type: 'text',
            text: '[ERROR] LLM response is not a valid JSON: ' + rawOutput,
          },
        ],
      };
    }

    const { query, params } = buildQuery('covid_dummy.covid19_open_data', json);
    try {
      const [rows] = await bigquery.query({ query, params });
      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(rows, null, 2),
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: '[ERROR] ' + (error instanceof Error ? error.message : String(error)),
          },
        ],
      };
    }
  })

server.tool(
  'json-to-nl',
  'Convert any JSON object into a key-value string (one per line).',
  { data: z.record(z.any()) },
  async ({ data }) => {
    const entries = Object.entries(data)
      .filter(([, v]) => v !== undefined && v !== null && v !== '')
      .map(([k, v]) => `${k}: ${v}`);
    const text = entries.length > 0
      ? `The provided information is as follows:\n${entries.join('\n')}\nIs this correct?`
      : '(no data)';
    return {
      content: [
        {
          type: 'text',
          text,
        },
      ],
    };
  }
)

async function main() {
  const transport = new StdioServerTransport()
  await server.connect(transport)
}

main().catch((error) => {
  console.error('Fatal error in main():', error)
  process.exit(1)
})