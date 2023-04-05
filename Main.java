import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    public static void main(String[] args) {
        System.out.println(LocalDateTime.now());
        // Здесь нужно подставить свои данные: путь до файла и название файла
        final long count = getCount("/Users/andreweseven/Desktop/уникальные строки/", "ip_addresses");
        System.out.println(count);
        System.out.println(LocalDateTime.now());
    }

    private static long getCount(String basePath, String fileName) {
        Map<Integer, FileWriter> fileWriterMap = new HashMap<>();
        List<String> filePaths = new ArrayList<>();
        final String uuid = UUID.randomUUID().toString();

        //Читаем файл, у каждой строки определяем его условный хэш по обственной хэш функции исходя из того что это ip адрес,
        // и затем распределяем строки в файлы в зависимости от полученного значения этого хэша
        try (BufferedReader br = new BufferedReader(new FileReader(basePath + fileName))) {
            String s = br.readLine();
            while (s != null) {
                final String[] values = s.split("\\.");
                int hash = Integer.parseInt(values[0]) + Integer.parseInt(values[1])
                        + Integer.parseInt(values[2]) + Integer.parseInt(values[3])
                        * Integer.parseInt(String.valueOf(values[1].charAt(0)));

                FileWriter fileWriter = fileWriterMap.get(hash);

                if (Objects.isNull(fileWriter)) {
                    final String filePath = uuid + hash + ".txt";
                    filePaths.add(filePath);
                    fileWriter = new FileWriter(basePath + filePath);
                    fileWriterMap.put(hash, fileWriter);
                }

                fileWriter.write(s);
                fileWriter.write('\n');

                s = br.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            fileWriterMap.values().forEach(fileWriter -> {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        AtomicLong count = new AtomicLong();

        final ExecutorService executorService = Executors.newFixedThreadPool(3);

        List<Future<Long>> futures = new ArrayList<>();
        // Пробегаем по всем файлам, смотрим на его размер, если размер меньше 300 мб, то отправляем задание на подсчет
        // количества уникальных строк в пул потоков, если значение больше 300мб, но меньше 1гб, то тогда считаем
        // уже текущим потоком(чтобы не перегрузить память и не было outOfMemory, ну а если файл больше 1гб, то тогда
        // идем текущим потоком в метод, который разделяем файлы на много других файлов по 100к уникальных отсортированных строк в каждом,
        // и затем соединяет эти файлы в 1 отсортированный файл, и в нем уже считает количество уникальных записей
        for (String filePath : filePaths) {
            File file = new File(basePath + filePath);

            if (file.length() < 314572800) {
                futures.add(executorService.submit(() -> getCountFromFile(basePath, filePath)));
            } else if (file.length() < 1073741824) {
                count.set(count.get() + getCountFromFile(basePath, filePath));
            } else {
                count.set(count.get() + merge(basePath, filePath));
            }
        }

        futures.forEach(future -> {
            try {
                count.set(count.get() + future.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.shutdown();

        filePaths.forEach(filePath -> new File(basePath + filePath).delete());

        return count.get();
    }

    private static long getCountFromFile(String basePath, String filePath) {
        // В этот метод попадают файлы максимум до 1гб, поэтому тут можно без пробелм посчитать количество уникальных строк
        // при помощи HashSet и по итогу отдать ее размер как количество уникальных строк в файле
        Set<String> set = new HashSet<>();

        try (BufferedReader br = new BufferedReader(new FileReader(basePath + filePath))) {
            String s = br.readLine();
            while (s != null) {
                set.add(s);
                s = br.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return set.size();
    }

    private static long merge(String basePath, String fileName) {
        // В этом методе происходит разделение крупного файла от 1гб на файлы по 500к отсортированных уникалных строк в каждом,
        // и затем идет переход в метод, где эти файлы начинают сливаться с друг с другом, при этом поддерживая сортировку, пока
        // в конечном итогу не получится один файл с отсортированными строками
        int filesCount = 1;
        Set<String> set = new TreeSet<>();
        final String uuid = UUID.randomUUID().toString();

        try (BufferedReader br = new BufferedReader(new FileReader(basePath + fileName))) {
            String s = br.readLine();
            while (s != null) {
                set.add(s);

                if (set.size() == 500000) {
                    try (FileWriter fw = new FileWriter(basePath + "mergeOut" + uuid + filesCount + ".txt")) {
                        for (String x : set) {
                            fw.write(x);
                            fw.write("\n");
                        }
                        filesCount++;
                        set.clear();
                    }
                }
                s = br.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (FileWriter fw = new FileWriter(basePath + "mergeOut" + uuid + filesCount + ".txt")) {
            for (String x : set) {
                fw.write(x);
                fw.write('\n');
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final ExecutorService executorService = Executors.newFixedThreadPool(50);

        final long count = merge(1, filesCount, uuid, null, basePath, executorService);

        executorService.shutdown();

        return count;
    }

    private static long getCountFromSortingFile(String uuid, String basePath) {
        // В этом методе происходит подсчет количества уникальных строк в файле с отсортированными строками
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(basePath + "mergeOut" + uuid + 1 + ".txt"))) {
            long count = 0;

            String s = bufferedReader.readLine();
            String current = "";

            while (s != null) {
                if (!s.equals(current)) {
                    count++;
                    current = s;
                }
                s = bufferedReader.readLine();
            }

            new File(basePath + "mergeOut" + uuid + 1 + ".txt").delete();
            return count;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static long merge(int first, int last, String uuid1, String uuid2, String basePath, ExecutorService executorService) {
        // В этом методе происходит слияние двух файлов с отсортированными строками в один, и так до тех пор, пока в итоге
        // не получится один файл ,и далее мы идем в метод подсчета уникальных строк в этом отсортированном файле
        if (uuid2 == null) {
            uuid2 = UUID.randomUUID().toString();
            List<Future<Long>> futures = new ArrayList<>();

            for (int i = first; i < last; ) {
                String finalUuid = uuid2;
                int finalI = i;
                futures.add(executorService.submit(() -> merge(finalI, finalI + 1, uuid1, finalUuid, basePath, executorService)));

                i++;
                if (i + 1 == last) {
                    try (FileWriter fw = new FileWriter(basePath + "mergeOut" + uuid2 + ((last / 2) + 1) + ".txt");
                         BufferedReader bufferedReader = new BufferedReader(new FileReader(basePath + "mergeOut" + uuid1 + i + ".txt"))) {

                        String s = bufferedReader.readLine();

                        while (s != null) {
                            fw.write(s);
                            fw.write("\n");
                            s = bufferedReader.readLine();
                        }

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    new File(basePath + "mergeOut" + uuid1 + last + ".txt").delete();
                    futures.add(null);

                    break;
                } else {
                    i++;
                }
            }

            futures
                    .stream()
                    .filter(Objects::nonNull)
                    .forEach(future -> {
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    });

            if (futures.size() > 1) {
                merge(1, futures.size(), uuid2, null, basePath, executorService);
            } else {
                return getCountFromSortingFile(uuid2, basePath);
            }
        } else {
            int j = (first / 2) + 1;

            for (int i = first + 1; i <= last; ) {
                try (BufferedReader bufferedReader1 = new BufferedReader(new FileReader(basePath + "mergeOut" + uuid1 + (i - 1) + ".txt"));
                     BufferedReader bufferedReader2 = new BufferedReader(new FileReader(basePath + "mergeOut" + uuid1 + (i) + ".txt"));
                     FileWriter fw = new FileWriter(basePath + "mergeOut" + uuid2 + j + ".txt")) {

                    String s1 = bufferedReader1.readLine();
                    String s2 = bufferedReader2.readLine();

                    while (s1 != null || s2 != null) {

                        if (s2 == null) {
                            fw.write(s1);
                            fw.write("\n");
                            s1 = bufferedReader1.readLine();
                            continue;
                        }

                        if (s1 == null) {
                            fw.write(s2);
                            fw.write("\n");
                            s2 = bufferedReader2.readLine();
                            continue;
                        }

                        if (s1.compareTo(s2) <= 0) {
                            fw.write(s1);
                            fw.write("\n");
                            s1 = bufferedReader1.readLine();
                        } else {
                            fw.write(s2);
                            fw.write("\n");
                            s2 = bufferedReader2.readLine();
                        }
                    }

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                new File(basePath + "mergeOut" + uuid1 + (i - 1) + ".txt").delete();
                new File(basePath + "mergeOut" + uuid1 + i + ".txt").delete();

                j++;
                i++;
                if (i == last) {
                    try (FileWriter fw = new FileWriter(basePath + "mergeOut" + uuid2 + j + ".txt");
                         BufferedReader bufferedReader = new BufferedReader(new FileReader(basePath + "mergeOut" + uuid1 + i + ".txt"))) {

                        String s = bufferedReader.readLine();

                        while (s != null) {
                            fw.write(s);
                            fw.write("\n");
                            s = bufferedReader.readLine();
                        }

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    new File(basePath + "mergeOut" + uuid1 + i + ".txt").delete();

                    break;
                } else {
                    i++;
                }
            }
        }
        return 0;
    }
}