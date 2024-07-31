CREATE TABLE IF NOT EXISTS public.example_table (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    value INT,
    created_at DATE
);

INSERT INTO public.example_table (id, name, value, created_at) VALUES
(1, 'example1', 100, '2024-07-30'),
(2, 'example2', 200, '2024-07-30');

SELECT * FROM public.example_table;
DROP TABLE public.example_table;