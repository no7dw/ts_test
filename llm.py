from typing import Any

from openai import AsyncOpenAI
from openai.types.chat import (
    ChatCompletionMessageParam,
    ChatCompletionSystemMessageParam,
    ChatCompletionUserMessageParam,
)


async def chat(
    query: str,
    model: str,
    system_message: str | None = None,
    client: AsyncOpenAI | None = None,
    messages: list[ChatCompletionMessageParam] | None = None,
    **kwargs: Any,
) -> str:
    if client is None:
        client = AsyncOpenAI()

    if not messages:
        messages = []

    if system_message:
        messages.insert(
            0, ChatCompletionSystemMessageParam(role="system", content=system_message)
        )

    messages += [
        ChatCompletionUserMessageParam(
            role="user",
            content=query,
        )
    ]

    chat_completion = await client.chat.completions.create(
        messages=messages,
        model=model,
        stream=False,
        **kwargs,
    )

    return "".join([choice.message.content or "" for choice in chat_completion.choices])
