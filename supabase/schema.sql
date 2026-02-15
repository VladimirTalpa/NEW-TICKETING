//Please run this SQL in your Supabase SQL editor to set up the necessary table and policies for the bot's state management.

create table if not exists public.bot_state (
  key text primary key,
  value jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);

alter table public.bot_state enable row level security;

drop policy if exists "service_role_all_bot_state" on public.bot_state;
create policy "service_role_all_bot_state"
on public.bot_state
for all
to service_role
using (true)
with check (true);

insert into public.bot_state (key, value)
values
  ('messages', '{}'::jsonb),
  ('ticketCounter', '{"last":0}'::jsonb),
  ('tickets_state', '{}'::jsonb),
  ('helpers', '{}'::jsonb),
  ('weekly', '{"weekStart":0,"helpers":{}}'::jsonb)
on conflict (key) do nothing;
