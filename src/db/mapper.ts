import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
// 프로젝트 루트의 mappers/ 디렉터리. src/db 와 dist/db 모두에서 ../../mappers 로 동일하게 해석됨.
const mappersDir = join(here, '..', '..', 'mappers');

/** 매퍼 네임스페이스 = 리포지터리. mappers/<ns>.xml 로 로드. */
const NAMESPACES = ['device', 'raw', 'domain', 'generic', 'location', 'error'];

const registry = new Map<string, Map<string, string>>();

function loadAll(): void {
  if (registry.size > 0) return;
  for (const ns of NAMESPACES) {
    const xml = readFileSync(join(mappersDir, `${ns}.xml`), 'utf8');
    registry.set(ns, parseMapper(xml));
  }
}

/** 단순 MyBatis 매퍼 XML 파싱: <select|insert|update|delete id="..">SQL</..> 추출. */
function parseMapper(xml: string): Map<string, string> {
  const map = new Map<string, string>();
  const noComments = xml.replace(/<!--[\s\S]*?-->/g, '');
  const re = /<(select|insert|update|delete)\b[^>]*\bid="([^"]+)"[^>]*>([\s\S]*?)<\/\1>/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(noComments)) !== null) {
    const sql = m[3].replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1').trim();
    map.set(m[2], sql);
  }
  return map;
}

export interface BoundQuery {
  text: string;
  values: unknown[];
}

/**
 * 매퍼에서 SQL을 가져와 `#{name}` → `$1,$2…` 위치 파라미터로 변환하고 값 배열을 만든다.
 * pg 파라미터라이즈드 쿼리를 사용하므로 PG에 안전.
 */
export function getQuery(namespace: string, id: string, params: Record<string, unknown> = {}): BoundQuery {
  loadAll();
  const stmt = registry.get(namespace)?.get(id);
  if (stmt == null) throw new Error(`mapper not found: ${namespace}.${id}`);
  const values: unknown[] = [];
  const text = stmt.replace(/#\{(\w+)\}/g, (_full, name: string) => {
    if (!(name in params)) throw new Error(`missing param '${name}' for ${namespace}.${id}`);
    values.push(params[name]);
    return `$${values.length}`;
  });
  return { text, values };
}
