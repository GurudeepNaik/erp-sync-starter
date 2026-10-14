-- ERP side of the sync. `customers` stands in for the ERP's own table; everything else belongs
-- to the sync and can live in a separate schema next to an existing ERP database.

create table if not exists customers (
    id            uuid primary key default gen_random_uuid(),
    crm_id        text unique,
    name          text not null,
    email         text,
    phone         text,
    credit_limit  numeric(12, 2) not null default 0,
    status        text not null default 'active',
    updated_at    timestamptz not null default now()
);

-- One row per customer: what the CRM looked like the last time we saw it, plus the lease that
-- gives one worker exclusive ownership of the customer while it talks to the CRM.
create table if not exists sync_state (
    customer_id   uuid primary key references customers (id) on delete cascade,
    create_key    uuid not null default gen_random_uuid(),  -- idempotency key for the CRM create
    crm_snapshot  jsonb,                                    -- last known CRM state (incl. version)
    lease_owner   text,
    lease_until   timestamptz
);

-- Transactional outbox: a change and the intent to publish it commit together, or not at all.
create table if not exists outbox (
    id            bigserial primary key,
    customer_id   uuid not null references customers (id) on delete cascade,
    reason        text not null,
    status        text not null default 'pending'
                  check (status in ('pending', 'sent', 'dead')),
    attempts      int not null default 0,
    available_at  timestamptz not null default now(),
    last_error    text,
    created_at    timestamptz not null default now(),
    done_at       timestamptz
);
create index if not exists outbox_pending on outbox (available_at, id) where status = 'pending';
create index if not exists outbox_customer on outbox (customer_id) where status = 'pending';

-- Inbox: webhooks are stored before they are acknowledged, de-duplicated by event id.
create table if not exists inbox (
    event_id      text primary key,
    crm_id        text not null,
    crm_version   bigint not null,
    payload       jsonb not null,
    received_at   timestamptz not null default now(),
    processed_at  timestamptz,
    outcome       text
);
create index if not exists inbox_unprocessed on inbox (received_at) where processed_at is null;

-- Capture every ERP write - including ones from legacy code that knows nothing about the sync.
-- Writes made *by* the sync set erpsync.origin = 'sync' for their transaction and are skipped,
-- which is what stops CRM changes from echoing straight back to the CRM.
create or replace function erpsync_capture() returns trigger language plpgsql as $$
begin
    insert into sync_state (customer_id) values (new.id) on conflict do nothing;
    if coalesce(current_setting('erpsync.origin', true), '') = 'sync' then
        return new;
    end if;
    if tg_op = 'UPDATE'
       and (new.name, new.email, new.phone, new.credit_limit, new.status)
           is not distinct from (old.name, old.email, old.phone, old.credit_limit, old.status) then
        return new;
    end if;
    insert into outbox (customer_id, reason) values (new.id, lower(tg_op));
    return new;
end $$;

create or replace function erpsync_touch() returns trigger language plpgsql as $$
begin
    if coalesce(current_setting('erpsync.origin', true), '') <> 'sync' then
        new.updated_at := now();
    end if;
    return new;
end $$;

drop trigger if exists customers_touch on customers;
create trigger customers_touch before update on customers
    for each row execute function erpsync_touch();

drop trigger if exists customers_capture on customers;
create trigger customers_capture after insert or update on customers
    for each row execute function erpsync_capture();
