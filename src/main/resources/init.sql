CREATE TABLE public.numbers (
    id INTEGER PRIMARY KEY,
    value INTEGER NOT NULL
);

CREATE SEQUENCE numbers__seq__id
    START WITH 1
    INCREMENT BY 1
    OWNED BY public.numbers.id;

ALTER TABLE public.numbers
    ALTER COLUMN id
        SET DEFAULT nextval('numbers__seq__id');