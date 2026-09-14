import type { SeoPageData } from "./pages";
import { snapshotFor } from "./snapshot";
import "./seo.css";

// Renders the pre-built guide string rather than JSX. The string is authored
// constants (see pages.ts), never user input - and sharing this serializer
// with the build is what keeps the prerendered page identical to this output.
export function SeoPage({ page }: { page: SeoPageData }) {
  return <div dangerouslySetInnerHTML={{ __html: snapshotFor(page) }} />;
}
