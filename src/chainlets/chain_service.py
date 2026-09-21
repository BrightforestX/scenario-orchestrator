"""Baseten Chainlet Service for Scenario Execution.

This chainlet receives scenario execution requests and performs inference
using open source models deployed on Baseten.
"""

from typing import Any


class ScenarioChainlet:
    """Chainlet for processing scenario prompts.

    This service handles incoming scenario execution requests,
    routes them to the appropriate model, and returns results.
    """

    def __init__(self) -> None:
        """Initialize the chainlet."""
        self._models: dict[str, Any] = {}
        self._default_model = "meta-llama/Llama-3.1-8B-Instruct"

    def load(self) -> None:
        """Load models on startup.

        This is called by Baseten when the chainlet starts.
        We use lazy loading to avoid loading all models upfront.
        """
        # Models are loaded lazily on first use
        pass

    def _get_model(self, model_name: str) -> Any:
        """Get or load a model by name.

        Args:
            model_name: The model identifier

        Returns:
            The loaded model pipeline
        """
        if model_name not in self._models:
            # In a real deployment, this would load from Baseten's model cache
            # For now, we use transformers pipeline
            from transformers import pipeline

            self._models[model_name] = pipeline(
                "text-generation",
                model=model_name,
                device_map="auto",
                torch_dtype="auto",
            )

        return self._models[model_name]

    def predict(self, request: dict[str, Any]) -> dict[str, Any]:
        """Process a scenario execution request.

        Args:
            request: Request containing prompt, parameters, and optional model

        Returns:
            Response with generated text and metadata
        """
        prompt = request.get("prompt", "")
        parameters = request.get("parameters", {})
        model_name = request.get("model", self._default_model)
        metadata = request.get("metadata", {})

        # Extract generation parameters
        max_tokens = parameters.get("max_tokens", 512)
        temperature = parameters.get("temperature", 0.7)
        top_p = parameters.get("top_p", 0.9)
        top_k = parameters.get("top_k", 50)

        try:
            model = self._get_model(model_name)

            # Generate response
            outputs = model(
                prompt,
                max_new_tokens=max_tokens,
                temperature=temperature,
                top_p=top_p,
                top_k=top_k,
                do_sample=temperature > 0,
                return_full_text=False,
            )

            generated_text = outputs[0]["generated_text"]

            return {
                "success": True,
                "output": generated_text,
                "model": model_name,
                "usage": {
                    "prompt_tokens": len(prompt.split()),  # Approximate
                    "completion_tokens": len(generated_text.split()),
                },
                "metadata": metadata,
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "model": model_name,
                "metadata": metadata,
            }


# Baseten expects a model class with load() and predict() methods
Model = ScenarioChainlet
