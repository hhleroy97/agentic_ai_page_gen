# CLAUDE.md  
### Project Guidelines for Agentic Local SEO Content Factory

## 1. Prompting Rules
- **Be concise.** Avoid overexplaining context. Stick to **direct, scoped prompts**.  
- **Anchor every output.** Always return files as complete code blocks, labeled with filenames.  
- **One task per prompt.** Do not try to handle unrelated features in a single request.  
- **Schema first.** Always conform to Pydantic models (`PageSpec`, etc.) when generating structured content. Reject or repair hallucinations.  

## 2. Consistency Rules
- **Deterministic outputs.** Keep temperature low (≤0.3) for code; use strict JSON schemas for SEO page specs.  
- **Stable naming.** Respect repo structure (`lambdas/`, `common/`, `site_templates/`, `sql/`). Don’t invent new folders.  
- **Idempotency.** Functions should safely overwrite existing files when rerun.  

## 3. Code Quality Rules
- **Type hints + docstrings** mandatory in Python.  
- **Structured logs** (JSON-style log lines).  
- **Small, pure functions** over giant monoliths.  
- **Security defaults.** No hardcoded secrets; use env vars (`BEDROCK_REGION`, bucket names).  

## 4. SEO & Content Rules
- Meta description ≤160 chars.  
- H1 ≤70 chars.  
- Minimum 800–1200 words per generated page.  
- JSON-LD must validate against schema.org.  
- Internal links: 3–5 relevant slugs, no self-links.  

## 5. Workflow Rules
- **Phase tasks (Day 1–3):**

### Day 1 – Infra & Data Layer
- [ ] Deploy SAM stack (S3, IAM, Lambdas, StepFn, Glue DB, Athena WG)  
- [ ] Load sample CSV to `raw/`, run Glue Crawler, confirm Athena tables  
- [ ] Implement `clean_transform` (Pandas + simple validations)  

### Day 2 – Agentic Generation & QC
- [ ] Implement Pydantic schema + generator prompt + QC/repair prompt  
- [ ] Store raw and repaired `PageSpec` JSON; write `trace.json`  
- [ ] Render Jinja templates → HTML; build sitemap/robots  

### Day 3 – Polish & Demo
- [ ] Publish 25–100 pages; add index page list  
- [ ] Add profiling queries; screenshot results  
- [ ] Record 60–90s demo; finalize notes  

- **One commit per feature.** Group changes logically.  
- **Prefer small iterations.** Ask for feedback early before building large sections.  

## 6. Collaboration Rules
- **Ask for clarification** if requirements are ambiguous.  
- **Summarize assumptions** before generating code.  
- **Do not remove existing files** unless explicitly instructed.  

## 7. Output Formatting
- Use fenced code blocks with **filename headers**.  
- When generating multiple files, show a **file tree first**.  
- Avoid commentary inside code blocks; keep explanations outside.  
