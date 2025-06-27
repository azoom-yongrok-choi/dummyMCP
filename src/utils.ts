import { z } from 'zod';

export function extractJsonFromLLMResponse(text: string, schema: z.ZodSchema): any | null {
  const jsonText = text.trim();
  try {
    return schema.parse(JSON.parse(jsonText));
  } catch {
    const match = jsonText.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (match) {
      try {
        return schema.parse(JSON.parse(match[1]));
      } catch {
        return null;
      }
    }
    return null;
  }
}

export function buildQuery(tableName: string, args: Record<string, any>, limit: number = 5) {
  const where: string[] = [];
  const params: Record<string, any> = {};

  for (const [key, value] of Object.entries(args)) {
    if (value !== undefined && value !== null && value !== "") {
      where.push(`${key} = @${key}`);
      params[key] = value;
    }
  }

  const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
  const query = `SELECT * FROM \`${tableName}\` ${whereClause} LIMIT ${limit}`;
  return { query, params };
}