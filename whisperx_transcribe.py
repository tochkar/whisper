import whisperx
import gc
import os

device = "cuda"
compute_type = "int8"  # change to "int8" if low on GPU mem (may reduce accuracy)
# 3. Assign speaker labels
diarize_model = whisperx.DiarizationPipeline(use_auth_token=os.environ['HF_TOKEN'], device=device)
whisper_models = {}


# audio_file must be mp3 or wav
def transcribe(audio_file, model_needed, language=None):
    batch_size = 8  # reduce if low on GPU mem
    asr_options = {
        'beam_size': 5, 'patience': None, 'length_penalty': 1.0,
        'temperatures': (0.0, 0.2, 0.4, 0.6000000000000001, 0.8, 1.0),
        'compression_ratio_threshold': 2.4, 'log_prob_threshold': -1.0, 'no_speech_threshold': 0.6,
        'condition_on_previous_text': False,
        'initial_prompt': None, 'suppress_tokens': [-1], 'suppress_numerals': False,
        "repetition_penalty": 1,
        "prompt_reset_on_temperature": 0.5,
        "no_repeat_ngram_size": 0,
        "max_new_tokens": None,
        "clip_timestamps": None,
        "hallucination_silence_threshold": None,
    }
    vad_options = {'vad_onset': 0.5, 'vad_offset': 0.363}
    if not model_needed in whisper_models:
        whisper_models[model_needed] = whisperx.load_model(
            model_needed, device=device, compute_type=compute_type, asr_options=asr_options, vad_options=vad_options,
            task='transcribe'
        )
    # 1. Transcribe with original whisper (batched)
    model = whisper_models[model_needed]
    audio = whisperx.load_audio(audio_file)
    transcribe_args = {}
    if language != None:
        transcribe_args["language"] = language
    result = model.transcribe(audio, batch_size=batch_size, **transcribe_args)
    # print(result["segments"]) # before alignment
    # delete model if low on GPU resources
    # import gc; gc.collect(); torch.cuda.empty_cache(); del model
    # 2. Align whisper output
    try:
        model_a, metadata = whisperx.load_align_model(language_code=result["language"], device=device)
        result = whisperx.align(result["segments"], model_a, metadata, audio, device, return_char_alignments=False)
    except:
        print("Fail to align", result["language"], "lang")
    # print(result["segments"]) # after alignment
    # delete model if low on GPU resources
    # import gc; gc.collect(); torch.cuda.empty_cache(); del model_a
    # add min/max number of speakers if known
    diarize_segments = diarize_model(audio_file)
    # diarize_model(audio_file, min_speakers=min_speakers, max_speakers=max_speakers)
    result = whisperx.assign_word_speakers(diarize_segments, result)
    # print(diarize_segments)
    # for segment in result["segments"]:
    writer = whisperx.utils.SubtitlesWriter("")
    writer.always_include_hours = True
    writer.decimal_marker = '.'
    items = []
    for start, end, text in writer.iterate_result(result, {
        "max_line_width": None,
        "max_line_count": None,
        "highlight_words": False
    }):
        parts = text.split(":")
        if len(parts) > 1:
            speaker, phrase = parts
        else:
            speaker = "[UNKNOWN]"
            phrase = text
        items.append({
            "start": start,
            "end": end,
            "speaker": speaker,
            "phrase": phrase
        })
    return items
