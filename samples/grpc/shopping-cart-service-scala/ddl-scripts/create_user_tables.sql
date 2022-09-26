
CREATE TABLE IF NOT EXISTS public.item_popularity (
    itemid VARCHAR(255) NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (itemid));
