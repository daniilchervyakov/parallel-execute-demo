using System.Collections.Concurrent;
using System.Threading.Channels;

namespace ClassLibrary1;

public static class Example
{
	public static async Task ProcessInParallelAsync<TItem>(
		// Можно обычный офк
		this IAsyncEnumerable<TItem> items, 
		Func<TItem, Task> processor,
		int degreeOfParallelism,
		// Опционально, опять же
		int inputBufferSize = int.MaxValue)
	{
		var inputChannel = Channel.CreateBounded<TItem>(inputBufferSize);

		var writeTask = Task.Run(async () =>
		{
			await foreach (var item in items)
			{
				await inputChannel.Writer.WriteAsync(item);
			}

			inputChannel.Writer.Complete();
		});

		var consumeTasks = Enumerable.Range(0, degreeOfParallelism)
			.Select(_ => Task.Run(async () =>
			{
				while (true)
				{
					try
					{
						var item = await inputChannel.Reader.ReadAsync();
						await processor(item);
					}
					catch (ChannelClosedException)
					{
						// Считали всё
						return;
					}
					catch(Exception e) {
						inputChannel.Writer.TryComplete(e)
					}
				}
			}));

		await Task.WhenAll(consumeTasks.Append(writeTask));
	}
	
	// Треш потоковая версия, псевдокод, не проверял работает ли
	public static IAsyncEnumerable<TResult> ProcessInParallelAsync<TItem, TResult>(
		// Можно обычный офк
		this IAsyncEnumerable<TItem> items, 
		Func<TItem, Task<TResult>> processor,
		int degreeOfParallelism,
		// Опционально, опять же
		int resultBufferSize = int.MaxValue,
		int inputBufferSize = 1)
	{
		var inputChannel = Channel.CreateBounded<TItem>(inputBufferSize);

		_ = Task.Run(async () =>
		{
			try
			{
				await foreach (var item in items)
				{
					await inputChannel.Writer.WriteAsync(item);
				}
			}
			catch (ChannelClosedException)
			{
				// Какой-то воркер сломался
			}

			inputChannel.Writer.Complete();
		});

		var resultChannel = Channel.CreateBounded<TResult>(resultBufferSize);

		for (var i = 0; i < degreeOfParallelism; i++)
		{
			_ = Task.Run(async () =>
			{
				while (true)
				{
					try
					{
						var item = await inputChannel.Reader.ReadAsync();
						var result = await processor(item);

						await resultChannel.Writer.WriteAsync(result);
					}
					catch (ChannelClosedException)
					{
						resultChannel.Writer.TryComplete();
					}
					catch (Exception e)
					{
						resultChannel.Writer.TryComplete(e);
						inputChannel.Writer.TryComplete();	
						
						// Было наблюдение, что если выкидывать что-то из fire and forget таски срабатывает эвент на финализацию таски с необработанным исключением и в Sentry начинает лететь всякое.
						return;
					}
				}
			});
		}

		// Тут мы будем процессить постепенно, в случае исключения обработаем только те, что были до него, после чего грохнемся.
		// Правда есть момент, что некоторые готовые результаты могут улететь в пустоту, если не смогут записаться в канал после того, как один из воркеров упал.
		return resultChannel.Reader.ReadAllAsync();
	}
	
	// Самое простое и адекватное решение
	public static async Task<List<TResult>> ProcessInParallelAsync<TItem, TResult>(
		this IEnumerable<TItem> items, 
		Func<TItem, Task<TResult>> processor,
		int degreeOfParallelism)
	{
		var queue = new ConcurrentQueue<TItem>(items);
		var exceptionCts = new CancellationTokenSource();
		
		var workers = Enumerable.Range(0, degreeOfParallelism)
			.Select(_ => Task.Run(async () =>
			{
				var localResult = new List<TResult>();
				
				while (queue.TryDequeue(out var item))
				{
					exceptionCts.ThrowIfCancellationRequested();
					localResult.Add(await processor(item));
				}

				return localResult;
			}))
			.ToList();
			
		while(workers.Any()) {
			var task = await Task.WhenAny(workers);
			
			if(!task.IsRanToCompletion) {
				exceptionCts.Cancel();
				// Rethrow exception
				await task;
			}

			workers.Remove(task);
		}
	}
}